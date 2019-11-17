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

package com.moilioncircle.redis.rdb.cli.monitor.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Counter;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Gauge;

/**
 * @author Jingqi Xu
 */
public class XMonitor implements Monitor {
    protected final String name;
    private final Map<String, XGauge> gauges = new ConcurrentHashMap<>(8);
    private final Map<String, XCounter> counters = new ConcurrentHashMap<>(8);

    public XMonitor(String name) {
        this.name = name;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final void set(String key, long value) {
        this.doSet(key, value);
    }

    @Override
    public final void add(String key, long count) {
        this.doAdd(key, count, -1);
    }

    @Override
    public final void add(String key, long count, long time) {
        this.doAdd(key, count, time);
    }

    @Override
    public Map<String, ? extends Gauge> getGauges() {
        return this.gauges;
    }

    @Override
    public Map<String, ? extends Counter> getCounters() {
        return this.counters;
    }

    public static final class FactoryImpl implements Monitor.Factory {
        @Override
        public Monitor create(String name) {
            return new XMonitor(name);
        }
    }

    protected void doSet(String k, final long v) {
        XGauge x = this.gauges.get(k);
        if (x == null) x = putIfAbsent(gauges, k, new XGauge());
        x.set(v);
    }

    protected void doAdd(String k, long c, long t) {
        XCounter x = counters.get(k);
        if (x == null) x = putIfAbsent(counters, k, new XCounter());
        x.add(c, t);
    }

    private static final <K, V> V putIfAbsent(Map<K, V> m, K k, V v) {
        final V r = m.putIfAbsent(k, v); return r != null ? r : v;
    }
}
