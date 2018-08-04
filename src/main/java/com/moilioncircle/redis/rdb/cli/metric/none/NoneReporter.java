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

package com.moilioncircle.redis.rdb.cli.metric.none;

import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricFilter;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.ScheduledReporter;
import io.dropwizard.metrics5.Timer;

import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;

import static io.dropwizard.metrics5.MetricFilter.ALL;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Baoyi Chen
 */
public class NoneReporter extends ScheduledReporter {
    
    protected NoneReporter(MetricRegistry registry, MetricFilter filter, ScheduledExecutorService executor, boolean shutdown) {
        super(registry, "none-reporter", filter, SECONDS, MILLISECONDS, executor, shutdown, emptySet());
    }
    
    public static NoneReporter.Builder forRegistry(MetricRegistry registry) {
        return new NoneReporter.Builder(registry);
    }
    
    public static class Builder {
        private final MetricRegistry registry;
        //
        private boolean shutdown;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        
        private Builder(MetricRegistry registry) {
            this.filter = ALL;
            this.executor = null;
            this.shutdown = true;
            this.registry = registry;
        }
        
        public NoneReporter.Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }
        
        public NoneReporter.Builder shutdownExecutorOnStop(boolean shutdown) {
            this.shutdown = shutdown;
            return this;
        }
        
        public NoneReporter.Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }
        
        public NoneReporter build() {
            return new NoneReporter(registry, filter, executor, shutdown);
        }
    }
    
    @Override
    public void report(SortedMap<MetricName, Gauge> gauges, SortedMap<MetricName, Counter> counters, SortedMap<MetricName, Histogram> histograms, SortedMap<MetricName, Meter> meters, SortedMap<MetricName, Timer> timers) {
        // NOP
    }
}
