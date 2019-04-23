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

package com.moilioncircle.redis.rdb.cli.net;

import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.util.type.Tuple3;
import com.moilioncircle.redis.replicator.Configuration;
import io.dropwizard.metrics5.MetricRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.moilioncircle.redis.rdb.cli.conf.NodeConfParser.slot;

/**
 * @author Baoyi Chen
 */
public class Endpoints implements Closeable {

    private final Set<Endpoint> set = new HashSet<>();
    private final Map<Short, Endpoint> endpoints = new HashMap<>();

    public Endpoints(List<String> lines, int pipe, MetricRegistry registry, Configuration configuration) {
        Function<Tuple3<String, Integer, String>, Endpoint> mapper = t -> {
            return new Endpoint(t.getV1(), t.getV2(), 0, pipe, registry, configuration);
        };
        new NodeConfParser<>(mapper).parse(lines, set, endpoints);
    }
    
    public Endpoint.RedisObject send(byte[] command, byte[]... args) {
        short slot = slot(args[0]);
        return endpoints.get(slot).send(command, args);
    }

    public void batch(boolean force, byte[] command, byte[]... args) {
        short slot = slot(args[0]);
        endpoints.get(slot).batch(force, command, args);
    }

    public void flush() {
        for (Endpoint endpoint : set) {
            endpoint.flush();
        }
    }

    @Override
    public void close() throws IOException {
        for (Endpoint endpoint : set) {
            endpoint.close();
        }
    }

    public void update(byte[] key) {
        short slot = slot(key);
        Endpoint v1 = endpoints.get(slot);
        replace(slot, v1, Endpoint.valueOf(v1));
    }

    public static void close(Endpoints endpoints) {
        if (endpoints == null) return;
        try {
            endpoints.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeQuietly(Endpoints endpoints) {
        if (endpoints == null) return;
        try {
            endpoints.close();
        } catch (Throwable e) {
        }
    }

    protected void replace(short slot, Endpoint v1, Endpoint v2) {
        set.remove(v1);
        set.add(v2);
        endpoints.put(slot, v2);
    }
}
