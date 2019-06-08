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

package com.moilioncircle.redis.rdb.cli.metric;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.metric.none.NoneReporter;
import com.moilioncircle.redis.rdb.cli.metric.prometheus.PrometheusReporter;
import com.moilioncircle.redis.rdb.cli.metric.prometheus.PrometheusSender;
import com.moilioncircle.redis.rdb.cli.metric.prometheus.PrometheusUriSender;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.ScheduledReporter;
import io.dropwizard.metrics5.Slf4jReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Baoyi Chen
 */
public class MetricReporterFactory {
    
    private static final Logger METRIC_LOGGER = LoggerFactory.getLogger("METRIC_LOGGER");
    
    public static ScheduledReporter create(Configure configure, MetricRegistry registry, MetricJob job) {
        switch (configure.getMetricGateway()) {
            case NONE:
                return NoneReporter.forRegistry(registry).build();
            case LOG:
                return Slf4jReporter.forRegistry(registry).outputTo(METRIC_LOGGER).build();
            case PROMETHEUS:
                PrometheusSender prometheus = new PrometheusUriSender(configure);
                return PrometheusReporter.forRegistry(registry).build(prometheus, job);
            default:
                throw new UnsupportedOperationException(configure.getMetricGateway().toString());
        }
    }
}
