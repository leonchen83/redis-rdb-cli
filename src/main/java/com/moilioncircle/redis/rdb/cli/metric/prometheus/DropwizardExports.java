package com.moilioncircle.redis.rdb.cli.metric.prometheus;

import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.Metric;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.Snapshot;
import io.dropwizard.metrics5.Timer;
import io.prometheus.client.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.regex.Pattern;

import static io.prometheus.client.Collector.MetricFamilySamples.Sample;
import static io.prometheus.client.Collector.Type.COUNTER;
import static io.prometheus.client.Collector.Type.GAUGE;
import static io.prometheus.client.Collector.Type.SUMMARY;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Baoyi Chen
 */
public class DropwizardExports extends Collector implements Collector.Describable {

    private static final Pattern METRIC_NAME_RE = Pattern.compile("[^a-zA-Z0-9:_]");

    private MetricRegistry registry;

    public DropwizardExports(MetricRegistry registry) {
        this.registry = registry;
    }

    private static List<MetricFamilySamples> fromCounter(MetricName metricName, Counter counter) {
        String name = normalize(metricName);
        Sample sample = new Sample(name, emptyList(), emptyList(), new Long(counter.getCount()).doubleValue());
        return singletonList(new MetricFamilySamples(name, GAUGE, message(metricName, counter), singletonList(sample)));
    }

    private static List<MetricFamilySamples> fromGauge(MetricName metricName, Gauge gauge) {
        String name = normalize(metricName);
        Object obj = gauge.getValue();
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
            return emptyList();
        }
        Sample sample = new Sample(name, emptyList(), emptyList(), value);
        return singletonList(new MetricFamilySamples(name, GAUGE, message(metricName, gauge), singletonList(sample)));
    }

    private static List<MetricFamilySamples> fromHistogram(MetricName metricName, Histogram histogram) {
        double factor = 1.0;
        long count = histogram.getCount();
        String name = normalize(metricName);
        Snapshot snapshot = histogram.getSnapshot();
        String message = message(metricName, histogram);
        List<Sample> samples = Arrays.asList(
                new Sample(name, singletonList("quantile"), singletonList("0.5"), snapshot.getMedian() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.75"), snapshot.get75thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.95"), snapshot.get95thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.98"), snapshot.get98thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.99"), snapshot.get99thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.999"), snapshot.get999thPercentile() * factor),
                new Sample(name + "_count", emptyList(), emptyList(), count)
        );
        return singletonList(new MetricFamilySamples(name, SUMMARY, message, samples));
    }

    private static List<MetricFamilySamples> fromTimer(MetricName metricName, Timer timer) {
        String name = normalize(metricName);
        long count = timer.getCount();
        Snapshot snapshot = timer.getSnapshot();
        double factor = 1.0d / SECONDS.toNanos(1L);
        String message = message(metricName, timer);
        List<Sample> samples = Arrays.asList(
                new Sample(name, singletonList("quantile"), singletonList("0.5"), snapshot.getMedian() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.75"), snapshot.get75thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.95"), snapshot.get95thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.98"), snapshot.get98thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.99"), snapshot.get99thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.999"), snapshot.get999thPercentile() * factor),
                new Sample(name + "_count", emptyList(), emptyList(), count)
        );
        return singletonList(new MetricFamilySamples(name, SUMMARY, message, samples));
    }

    private static List<MetricFamilySamples> fromMeter(MetricName metricName, Meter meter) {
        String name = normalize(metricName);
        List<Sample> samples = singletonList(new Sample(name + "_total", emptyList(), emptyList(), meter.getCount()));
        return singletonList(new MetricFamilySamples(name + "_total", COUNTER, message(metricName, meter), samples));
    }

    private static String normalize(MetricName metricName) {
        String name = METRIC_NAME_RE.matcher(metricName.getKey()).replaceAll("_");
        if (!name.isEmpty() && Character.isDigit(name.charAt(0))) name = "_" + name;
        return name;
    }

    private static String message(MetricName metricName, Metric metric) {
        String name = metricName.getKey();
        String type = metric.getClass().getName();
        return String.format("Generated from Dropwizard metric import (metric=%s, type=%s)", name, type);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> result = new ArrayList<>();
        for (SortedMap.Entry<MetricName, Timer> entry : registry.getTimers().entrySet()) {
            result.addAll(fromTimer(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Meter> entry : registry.getMeters().entrySet()) {
            result.addAll(fromMeter(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Gauge> entry : registry.getGauges().entrySet()) {
            result.addAll(fromGauge(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Counter> entry : registry.getCounters().entrySet()) {
            result.addAll(fromCounter(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Histogram> entry : registry.getHistograms().entrySet()) {
            result.addAll(fromHistogram(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return emptyList();
    }
}
