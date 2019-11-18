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

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.impl.InfluxdbGateway;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.impl.LogGateway;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.impl.NoneGateway;

/**
 * @author Baoyi Chen
 */
public class MetricGatewayFactory {

    public static MetricGateway create(Configure configure) {
        switch (configure.getMetricGateway()) {
            case NONE:
                return new NoneGateway();
            case LOG:
                return new LogGateway();
            case INFLUXDB:
                return new InfluxdbGateway(configure);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
