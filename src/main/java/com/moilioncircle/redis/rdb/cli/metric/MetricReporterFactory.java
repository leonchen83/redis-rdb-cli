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
    
    public static ScheduledReporter create(Configure configure, MetricRegistry registry, String job) {
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
