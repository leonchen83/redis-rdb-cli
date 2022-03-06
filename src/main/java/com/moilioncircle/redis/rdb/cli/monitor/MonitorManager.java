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

import static com.moilioncircle.redis.replicator.util.Concurrents.terminateQuietly;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.MetricGateway;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.MetricGatewayFactory;
import com.moilioncircle.redis.rdb.cli.monitor.points.CounterPoint;
import com.moilioncircle.redis.rdb.cli.monitor.points.GaugePoint;
import com.moilioncircle.redis.rdb.cli.util.XThreadFactory;

/**
 * @author Baoyi Chen
 */
public class MonitorManager implements Closeable {
    //
    private static final Logger logger = LoggerFactory.getLogger(MonitorManager.class);

    private MetricGateway metricGateway;
    private ScheduledExecutorService executor;
    private long timeout = SECONDS.toMillis(5);

    public MonitorManager(Configure configure) {
        this.metricGateway = MetricGatewayFactory.create(configure);
        this.executor = Executors.newSingleThreadScheduledExecutor(new XThreadFactory("monitor-scheduler"));
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void report() {
        List<GaugePoint<?>> gauges = new ArrayList<>();
        List<CounterPoint<?>> counters = new ArrayList<>();
        try {
            for (Monitor monitor : MonitorFactory.getAllMonitors().values()) {
                for (final Map.Entry<MonitorKey, ? extends Gauge<Long>> e : monitor.getLongGauges().entrySet()) {
                    final Gauge<Long> gauge = e.getValue().reset();
                    if (gauge == null) continue;
                    gauges.add(GaugePoint.valueOf(monitor, e.getKey(), gauge));
                }
    
                for (final Map.Entry<MonitorKey, ? extends Gauge<String>> e : monitor.getStringGauges().entrySet()) {
                    final Gauge<String> gauge = e.getValue().reset();
                    if (gauge == null) continue;
                    gauges.add(GaugePoint.valueOf(monitor, e.getKey(), gauge));
                }
    
                for (final Map.Entry<MonitorKey, ? extends Gauge<Double>> e : monitor.getDoubleGauges().entrySet()) {
                    final Gauge<Double> gauge = e.getValue().reset();
                    if (gauge == null) continue;
                    gauges.add(GaugePoint.valueOf(monitor, e.getKey(), gauge));
                }
    
                for (final Map.Entry<MonitorKey, ? extends Counter<Long>> e : monitor.getLongCounters().entrySet()) {
                    final Counter<Long> counter = e.getValue().reset();
                    if (counter == null) continue;
                    counters.add(CounterPoint.valueOf(monitor, e.getKey(), counter));
                }
    
                for (final Map.Entry<MonitorKey, ? extends Counter<Double>> e : monitor.getDoubleCounters().entrySet()) {
                    final Counter<Double> counter = e.getValue().reset();
                    if (counter == null) continue;
                    counters.add(CounterPoint.valueOf(monitor, e.getKey(), counter));
                }
            }
            metricGateway.save(gauges, counters);
        } catch (Throwable e) {
            logger.error("failed to report points.", e);
        }
    }
    
    public void reset(String... measurements) {
        if (measurements == null || measurements.length == 0) return;
        logger.debug("reset measurement {}", Arrays.toString(measurements));
        for (String measurement : measurements) metricGateway.reset(measurement);
    }
    
    public void open(String... measurements) {
        reset(measurements);
        logger.debug("open monitor manager");
        executor.scheduleWithFixedDelay(this::report, timeout, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        terminateQuietly(executor, 0, TimeUnit.MILLISECONDS);
        report(); 
        metricGateway.close();
        logger.debug("close monitor manager");
    }

    public static void closeQuietly(MonitorManager manager) {
        try {
            if (manager != null) manager.close();
        } catch (Throwable e) {
        }
    }
}
