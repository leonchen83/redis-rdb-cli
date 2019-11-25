/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.rdb.cli.net.impl;

import static com.moilioncircle.redis.rdb.cli.conf.NodeConfParser.slot;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.util.type.Tuple3;

/**
 * @author Baoyi Chen
 */
public class XEndpoints implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(XEndpoints.class);

    private final Configure configure;
    private final Configuration configuration;
    private Set<XEndpoint> index1 = new HashSet<>();
    private List<String> clusterNodes = new ArrayList<>();
    private Map<Short, XEndpoint> index2 = new HashMap<>(16384);

    public XEndpoints(List<String> lines, int pipe, boolean statistics, Configuration configuration, Configure configure) {
        this.clusterNodes = lines;
        this.configure = configure;
        this.configuration = configuration;
        Function<Tuple3<String, Integer, String>, XEndpoint> mapper = t -> {
            return new XEndpoint(t.getV1(), t.getV2(), 0, pipe, statistics, configuration, configure);
        };
        new NodeConfParser<>(mapper).parse(lines, index1, index2);
    }
    
    public List<String> getClusterNodes() {
        return clusterNodes;
    }
    
    public void ping(boolean force) {
        for (XEndpoint prev : new HashSet<>(index1)) {
            try {
                prev.batch(force, "PING".getBytes());
            } catch (Throwable e) {
                updateQuietly(prev);
                break;
            }
        }
    }
    
    public RedisObject send(byte[] command, byte[]... args) {
        short slot = slot(args[0]);
        return index2.get(slot).send(command, args);
    }

    public void batch(boolean force, short slot, byte[] command, byte[]... args) {
        index2.get(slot).batch(force, command, args);
    }

    public void flushQuietly() {
        for (XEndpoint endpoint : index1) {
            endpoint.flushQuietly();
        }
    }

    @Override
    public void close() throws IOException {
        for (XEndpoint endpoint : index1) {
            endpoint.close();
        }
    }

    public void updateQuietly(short slot) {
        try {
            update(slot);
        } catch (Throwable e) {
        }
    }

    public void update(short slot) {
        update(index2.get(slot));
    }
    
    public void updateQuietly(XEndpoint endpoint) {
        try {
            update(endpoint);
        } catch (Throwable e) {
        }
    }

    public void update(XEndpoint endpoint) {
        logger.debug("update cluster view. failed node {}:{}, prev {}", endpoint.getHost(), endpoint.getPort(), index1);
        try {
            XEndpoint next = XEndpoint.valueOf(endpoint, 0);
            RedisObject r= next.send("role".getBytes());
            RedisObject[] array = r.getArray();
            if (array[0].getString().equals("master")) {
                // master
                replace(endpoint.getSlots(), endpoint, next);
            } else {
                // slave
                String host = array[1].getString();
                int port = array[2].getNumber().intValue();
                next = XEndpoint.valueOf(host, port, 0, next);
                replace(endpoint.getSlots(), endpoint, next);
            }
        } catch (Throwable e) {
            // FAILOVER PROCESS
            logger.debug("FAILOVER PROCESS!");
            
            // when all above failover mechanism failed.
            // need update all cluster nodes view
            
            // 1 get cluster nodes view
            List<String> lines = null;
            for (XEndpoint prev : index1) {
                try {
                    RedisObject r = prev.send("cluster".getBytes(), "nodes".getBytes());
                    if (r.type.isError()) {
                        // try next endpoint
                        continue;
                    }
                    String config = r.getString();
                    lines = Arrays.asList(config.split("\n"));
                    break;
                } catch (Throwable error) {
                    continue;
                }
            }
            
            // 2 if all endpoints failed exit.
            if (lines == null) {
                // unrecoverable error
                logger.error("can't connect to any of cluster nodes");
                return; // try again next loop
            }
            
            // 3 parse nodes info
            Set<DummyEndpoint> next1 = new HashSet<>();
            Map<Short, DummyEndpoint> next2 = new HashMap<>(16384);
            new NodeConfParser<DummyEndpoint>(tuple -> {
                return new DummyEndpoint(tuple.getV1(), tuple.getV2());
            }).parse(lines, next1, next2);
            
            // 4 update all cluster nodes view
            merge(next1, next2, lines);
            logger.debug("merged cluster view. next {}", index1);
        }
    }

    private void merge(Set<DummyEndpoint> next1, Map<Short, DummyEndpoint> next2, List<String> lines) {
        Set<XEndpoint> n1 = new HashSet<>();
        Map<Short, XEndpoint> n2 = new HashMap<>(16384);
        
        for (XEndpoint endpoint : index1) {
            if (next1.contains(endpoint)) {
                n1.add(endpoint); // reuse old endpoint
            } else {
                XEndpoint.closeQuietly(endpoint);
            }
        }
        for (DummyEndpoint dummy : next1) {
            if (!n1.contains(dummy)) {
                n1.add(DummyEndpoint.valueOf(dummy, configuration, configure)); // new endpoint
            }
        }
        
        for (XEndpoint endpoint : n1) {
            for(Short slot : endpoint.getSlots()) {
                n2.put(slot, endpoint);
            }
        }
        
        if (n2.size() != 16384) {
            // unrecoverable error
            System.out.println("unsupported migrating importing slot");
            System.exit(-1);
        }
        
        this.index1 = n1;
        this.index2 = n2;
        this.clusterNodes = lines;
    }

    public static void close(XEndpoints endpoints) {
        if (endpoints == null) return;
        try {
            endpoints.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeQuietly(XEndpoints endpoints) {
        if (endpoints == null) return;
        try {
            endpoints.close();
        } catch (Throwable e) {
        }
    }

    protected void replace(List<Short> slots, XEndpoint v1, XEndpoint v2) {
        index1.remove(v1);
        index1.add(v2);
        for (short slot : slots) {
            index2.put(slot, v2);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (XEndpoint endpoint : index1) {
            builder.append(endpoint.toString());
            builder.append(",");
        }
        return builder.toString();
    }
}
