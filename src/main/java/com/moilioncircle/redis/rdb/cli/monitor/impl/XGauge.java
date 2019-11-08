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

import java.util.concurrent.atomic.AtomicLong;

import com.moilioncircle.redis.rdb.cli.monitor.entity.Gauge;

/**
 * @author Jingqi Xu
 */
public final class XGauge implements Gauge {

    private final AtomicLong gauge = new AtomicLong(0);

    @Override
    public long getGauge() {
        return this.gauge.get();
    }

    @Override
    public Gauge reset() {
        long v = gauge.get();
        if (v == 0) return null; else return new ImmutableXGauge(v);
    }

    void set(final long value) {
        gauge.set(value);
    }

    private static class ImmutableXGauge implements Gauge {
        private final long value;

        public ImmutableXGauge(long v) {
            this.value = v;
        }

        @Override
        public Gauge reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getGauge() {
            return value;
        }
    }
}
