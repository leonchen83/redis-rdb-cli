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

package com.moilioncircle.redis.rdb.cli.monitor.gateway.impl;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.monitor.gateway.MetricGateway;
import com.moilioncircle.redis.rdb.cli.monitor.points.CounterPoint;
import com.moilioncircle.redis.rdb.cli.monitor.points.GaugePoint;

/**
 * @author Baoyi Chen
 */
public class LogGateway implements MetricGateway {
    
    private static final Logger logger = LoggerFactory.getLogger("METRIC_LOGGER");

    @Override
    public void reset(String measurement) {
        
    }

    @Override
    public boolean save(List<GaugePoint<?>> gauges, List<CounterPoint<?>> counters) {
        for (GaugePoint<?> point : gauges) {
            logger.info(point.toString());
        }
        for (CounterPoint<?> point : counters) {
            logger.info(point.toString());
        }
        return true;
    }

    @Override
    public void close() throws IOException {

    }
}
