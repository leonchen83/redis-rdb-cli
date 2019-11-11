/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.moilioncircle.redis.rdb.cli.monitor.impl.XMonitor;

/**
 * @author Baoyi Chen
 */
public class MonitorFactory {

    private static final Monitor.Factory FACTORY = new XMonitor.FactoryImpl();
    private static final ConcurrentMap<String, Monitor> CACHE = new ConcurrentHashMap<>(64);

    public static Map<String, Monitor> getAllMonitors() {
        final Map<String, Monitor> r = new HashMap<String, Monitor>(CACHE);
        return r;
    }

    public static Monitor getMonitor(String name) {
        Monitor r = CACHE.get(name);
        if (r == null) r = CACHE.putIfAbsent(name, FACTORY.create(name));
        return r;
    }
}
