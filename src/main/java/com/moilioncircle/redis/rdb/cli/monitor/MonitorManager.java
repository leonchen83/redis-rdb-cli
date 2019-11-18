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

import static com.moilioncircle.redis.rdb.cli.glossary.Gateway.INFLUXDB;
import static com.moilioncircle.redis.replicator.util.Concurrents.terminateQuietly;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.Gateway;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Counter;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Gauge;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.Influxdb;

/**
 * @author Baoyi Chen
 */
public class MonitorManager implements Closeable {
    //
    private static final Logger logger = LoggerFactory.getLogger(MonitorManager.class);

    private Gateway gateway;
    private Configure configure;
    private Influxdb influxdb;
    private ScheduledExecutorService executor;
    private long timeout = SECONDS.toMillis(5);

    public MonitorManager(Configure configure) {
        this.configure = configure;
        this.influxdb = new Influxdb(configure);
        this.gateway = this.configure.getMetricGateway();
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void report() {
        List<MonitorPoint> points = new ArrayList<>();
        try {
            for (Monitor monitor : MonitorFactory.getAllMonitors().values()) {
                for (final Map.Entry<String, ? extends Gauge> e : monitor.getGauges().entrySet()) {
                    final Gauge gauge = e.getValue().reset();
                    if (gauge == null) continue;
                    points.add(MonitorPoint.valueOf(monitor, e.getKey(), gauge));
                }

                for (final Map.Entry<String, ? extends Counter> e : monitor.getCounters().entrySet()) {
                    final Counter counter = e.getValue().reset();
                    if (counter == null) continue;
                    points.add(MonitorPoint.valueOf(monitor, e.getKey(), counter));
                }
            }
            if (gateway == INFLUXDB) influxdb.save(points);
        } catch (Throwable e) {
            logger.error("failed to report points {}.", points, e);
        }
    }
    
    public void reset(String measurement) {
        logger.debug("reset measurement {}", measurement);
        if (gateway == INFLUXDB) {
            influxdb.reset(measurement);
        }
    }

    public void open() {
        logger.debug("open monitor manager");
        executor.scheduleWithFixedDelay(this::report, timeout, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        terminateQuietly(executor, 0, TimeUnit.MILLISECONDS);
        if (gateway == INFLUXDB && influxdb != null) {
            report(); 
            influxdb.close();
        }
        logger.debug("close monitor manager");
    }

    public static void closeQuietly(MonitorManager manager) {
        try {
            if (manager != null) manager.close();
        } catch (Throwable e) {
        }
    }
}
