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

package com.moilioncircle.redis.rdb.cli.monitor.gateway;

import java.io.Closeable;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.monitor.points.CounterPoint;
import com.moilioncircle.redis.rdb.cli.monitor.points.GaugePoint;

/**
 * @author Baoyi Chen
 */
public interface MetricGateway extends Closeable {
    
    void reset(String measurement);
    boolean save(List<GaugePoint<?>> gauges, List<CounterPoint<?>> counters);
}
