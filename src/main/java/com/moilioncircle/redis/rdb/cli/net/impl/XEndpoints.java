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
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.CLUSTER;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.NODES;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.PING;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ROLE;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;
import com.moilioncircle.redis.rdb.cli.util.Collections;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.util.type.Tuple3;

/**
 * @author Baoyi Chen
 */
public class XEndpoints implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(XEndpoints.class);

    private final int pipe;
    private final Configuration configuration;
    private Set<XEndpoint> index1 = new HashSet<>();
    private List<String> clusterNodes = new ArrayList<>();
    private Map<Short, XEndpoint> index2 = new HashMap<>(32768);

    public XEndpoints(List<String> lines, int pipe, boolean statistics, Configuration configuration) {
        this.pipe = pipe;
        this.clusterNodes = lines;
        this.configuration = configuration;
        Function<Tuple3<String, Integer, String>, XEndpoint> mapper = t -> {
            return new XEndpoint(t.getV1(), t.getV2(), 0, pipe, statistics, configuration);
        };
        NodeConfParser.parse(lines, index1, index2, mapper);
    
        if (index2.size() != 16384) {
            throw new UnsupportedOperationException("slots size : " + index2.size() + ", expected 16384.");
        }
    }
    
    public List<String> getClusterNodes() {
        return clusterNodes;
    }
    
    public void ping(boolean force) {
        for (XEndpoint prev : new HashSet<>(index1)) {
            try {
                prev.batch(force, PING);
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
    
    public boolean broadcast(byte[] command, byte[]... args) {
        boolean result = true;
        for (XEndpoint prev : new HashSet<>(index1)) {
            try {
                prev.send(command, args);
            } catch (Throwable e) {
                updateQuietly(prev);
                result = false;
            }
        }
        return result;
    }

    public void batch(boolean force, short slot, byte[] command, byte[]... args) {
        index2.get(slot).batch(force, command, args);
    }
    
    public void batch(boolean force, short slot, ByteBuffers command, ByteBuffers... args) {
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
            RedisObject r= next.send(ROLE);
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
            failover();
        }
    }
    
    private void failover() {
        // FAILOVER PROCESS
        logger.debug("FAILOVER PROCESS!");
        
        // when all above failover mechanism failed.
        // need update all cluster nodes view
        
        // 1 get cluster nodes view
        String config = null;
        List<String> lines = null;
        for (XEndpoint prev : index1) {
            try {
                RedisObject r = prev.send(CLUSTER, NODES);
                if (r.type.isError()) {
                    // try next endpoint
                    continue;
                }
                config = r.getString();
                lines = Collections.ofList(config.split("\n"));
                break;
            } catch (Throwable error) {
            }
        }
        
        // 2 if all endpoints failed exit.
        if (lines == null) {
            logger.error("can't connect to any of cluster nodes");
            return;
        }
        
        // 3 parse nodes info
        Set<DummyEndpoint> next1 = new HashSet<>();
        Map<Short, DummyEndpoint> next2 = new HashMap<>(32768);
        try {
            NodeConfParser.parse(lines, next1, next2, tuple -> {
                return new DummyEndpoint(tuple.getV1(), tuple.getV2());
            });
    
            if (next2.size() != 16384) {
                throw new UnsupportedOperationException("slots size : " + next2.size() + ", expected 16384.");
            }
        } catch (Throwable cause) {
            return;
        }
        
        // 4 update all cluster nodes view
        merge(next1, next2, lines, config);
        logger.debug("merged cluster view. next {}", index1);
    }
    
    private void merge(Set<DummyEndpoint> next1, Map<Short, DummyEndpoint> next2, List<String> lines, String config) {
        Set<XEndpoint> n1 = new HashSet<>();
        Map<Short, XEndpoint> n2 = new HashMap<>(16384);
        // 1. close broken endpoint
        for (XEndpoint endpoint : index1) {
            if (!next1.contains(endpoint)) {
                XEndpoint.closeQuietly(endpoint);
                continue;
            }
            
            try {
                RedisObject r = endpoint.send(PING);
                if (r.type.isError()) {
                    XEndpoint.closeQuietly(endpoint);
                } else {
                    n1.add(endpoint); // reuse old endpoint
                }
            } catch (Throwable e) {
                XEndpoint.closeQuietly(endpoint);
            }
        }
        
        // 2. create connection for new endpoint
        for (DummyEndpoint dummy : next1) {
            if (!n1.contains(dummy)) {
                XEndpoint endpoint = DummyEndpoint.valueOfQuietly(dummy, configuration, pipe);
                if (endpoint != null) n1.add(endpoint); // new endpoint
            }
        }
        
        // 3. create slot index
        for (XEndpoint endpoint : n1) {
            for(Short slot : endpoint.getSlots()) {
                n2.put(slot, endpoint);
            }
        }
        
        // 4. validate slot
        if (n2.size() != 16384) {
            // unrecoverable error
            logger.error("unsupported migrating importing slot. covered slots: [{}], cluster config: [{}]", n2.size(), config);
            System.out.println("unsupported migrating importing slot. covered slots:" + n2.size());
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
