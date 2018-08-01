package com.moilioncircle.redis.rdb.cli.metric.prometheus;

import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricFilter;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.ScheduledReporter;
import io.dropwizard.metrics5.Timer;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;

import static io.dropwizard.metrics5.MetricFilter.ALL;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Baoyi Chen
 */
public class PrometheusReporter extends ScheduledReporter {


    public static PrometheusReporter.Builder forRegistry(MetricRegistry registry) {
        return new PrometheusReporter.Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        //
        private MetricFilter filter;
        private boolean shutdownExecutorOnStop;
        private ScheduledExecutorService executor;

        private Builder(MetricRegistry registry) {
            this.filter = ALL;
            this.executor = null;
            this.registry = registry;
            this.shutdownExecutorOnStop = true;
        }

        public PrometheusReporter.Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public PrometheusReporter.Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public PrometheusReporter.Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        public PrometheusReporter build(PrometheusSender sender, String job) {
            PrometheusReporter reporter = new PrometheusReporter(registry, filter, executor, shutdownExecutorOnStop);
            reporter.job = job;
            reporter.sender = sender;
            reporter.registry = registry;
            try {
                reporter.sender.delete(job);
            } catch (IOException e) {
                LOGGER.warn("Unable to delete from Prometheus {}, job {}", sender, job, e);
            }
            return reporter;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);

    private String job;
    private PrometheusSender sender;
    private MetricRegistry registry;

    protected PrometheusReporter(MetricRegistry registry, MetricFilter filter, ScheduledExecutorService executor, boolean shutdown) {
        super(registry, "prometheus-reporter", filter, SECONDS, MILLISECONDS, executor, shutdown, emptySet());
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void report(SortedMap<MetricName, Gauge> gauges,
                       SortedMap<MetricName, Counter> counters,
                       SortedMap<MetricName, Histogram> histograms,
                       SortedMap<MetricName, Meter> meters,
                       SortedMap<MetricName, Timer> timers) {
        CollectorRegistry registry = new CollectorRegistry();
        new DropwizardExports(this.registry).register(registry);
        try {
            sender.pushAdd(registry, job);
        } catch (IOException e) {
            LOGGER.warn("Unable to report to Prometheus {}", sender, e);
        }
    }
}
