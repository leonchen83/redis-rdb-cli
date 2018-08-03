package com.moilioncircle.redis.rdb.cli.metric.prometheus;

import com.moilioncircle.redis.rdb.cli.util.Tuples;
import com.moilioncircle.redis.rdb.cli.util.type.Tuple2;
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
import java.util.Map;
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

    private static List<MetricFamilySamples> counter(MetricName metricName, Counter counter) {
        String name = normalize(metricName);
        Tuple2<List<String>, List<String>> tuple = tags(metricName);
        Sample sample = new Sample(name, tuple.getV1(), tuple.getV2(), new Long(counter.getCount()).doubleValue());
        return singletonList(new MetricFamilySamples(name, GAUGE, message(metricName, counter), singletonList(sample)));
    }

    private static List<MetricFamilySamples> gauge(MetricName metricName, Gauge gauge) {
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
        Tuple2<List<String>, List<String>> tuple = tags(metricName);
        Sample sample = new Sample(name, tuple.getV1(), tuple.getV2(), value);
        return singletonList(new MetricFamilySamples(name, GAUGE, message(metricName, gauge), singletonList(sample)));
    }

    private static List<MetricFamilySamples> histogram(MetricName metricName, Histogram histogram) {
        long count = histogram.getCount();
        String name = normalize(metricName);
        Snapshot snapshot = histogram.getSnapshot();
        String message = message(metricName, histogram);
        List<Sample> samples = Arrays.asList(
                new Sample(name, singletonList("quantile"), singletonList("0.001"), snapshot.getValue(0.001)),
                new Sample(name, singletonList("quantile"), singletonList("0.01"), snapshot.getValue(0.01)),
                new Sample(name, singletonList("quantile"), singletonList("0.1"), snapshot.getValue(0.1)),
                new Sample(name, singletonList("quantile"), singletonList("0.25"), snapshot.getValue(0.25)),
                new Sample(name, singletonList("quantile"), singletonList("0.5"), snapshot.getMedian()),
                new Sample(name, singletonList("quantile"), singletonList("0.75"), snapshot.get75thPercentile()),
                new Sample(name, singletonList("quantile"), singletonList("0.95"), snapshot.get95thPercentile()),
                new Sample(name, singletonList("quantile"), singletonList("0.98"), snapshot.get98thPercentile()),
                new Sample(name, singletonList("quantile"), singletonList("0.99"), snapshot.get99thPercentile()),
                new Sample(name, singletonList("quantile"), singletonList("0.999"), snapshot.get999thPercentile()),
                new Sample(name + "_count", emptyList(), emptyList(), count),
                new Sample(name + "_max", emptyList(), emptyList(), snapshot.getMax()),
                new Sample(name + "_min", emptyList(), emptyList(), snapshot.getMin()),
                new Sample(name + "_mean", emptyList(), emptyList(), snapshot.getMean())
        );
        return singletonList(new MetricFamilySamples(name, SUMMARY, message, samples));
    }

    private static List<MetricFamilySamples> timer(MetricName metricName, Timer timer) {
        String name = normalize(metricName);
        long count = timer.getCount();
        Snapshot snapshot = timer.getSnapshot();
        double factor = 1.0d / SECONDS.toNanos(1L);
        String message = message(metricName, timer);
        List<Sample> samples = Arrays.asList(
                new Sample(name, singletonList("quantile"), singletonList("0.001"), snapshot.getValue(0.001) * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.01"), snapshot.getValue(0.01) * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.1"), snapshot.getValue(0.1) * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.25"), snapshot.getValue(0.25) * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.5"), snapshot.getMedian() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.75"), snapshot.get75thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.95"), snapshot.get95thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.98"), snapshot.get98thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.99"), snapshot.get99thPercentile() * factor),
                new Sample(name, singletonList("quantile"), singletonList("0.999"), snapshot.get999thPercentile() * factor),
                new Sample(name + "_count", emptyList(), emptyList(), count),
                new Sample(name + "_max", emptyList(), emptyList(), snapshot.getMax()),
                new Sample(name + "_min", emptyList(), emptyList(), snapshot.getMin()),
                new Sample(name + "_mean", emptyList(), emptyList(), snapshot.getMean())
        );
        return singletonList(new MetricFamilySamples(name, SUMMARY, message, samples));
    }

    private static List<MetricFamilySamples> meter(MetricName metricName, Meter meter) {
        String name = normalize(metricName);
        Tuple2<List<String>, List<String>> tuple = tags(metricName);
        List<Sample> samples = singletonList(new Sample(name + "_total", tuple.getV1(), tuple.getV2(), meter.getCount()));
        return singletonList(new MetricFamilySamples(name + "_total", COUNTER, message(metricName, meter), samples));
    }

    private static String normalize(MetricName metricName) {
        String name = METRIC_NAME_RE.matcher(metricName.getKey()).replaceAll("_");
        if (!name.isEmpty() && Character.isDigit(name.charAt(0))) name = "_" + name;
        return name;
    }
    
    private static Tuple2<List<String>, List<String>> tags(MetricName name) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, String> entry : name.getTags().entrySet()) {
            keys.add(entry.getKey());
            values.add(entry.getValue());
        }
        return Tuples.of(keys, values);
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
            result.addAll(timer(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Meter> entry : registry.getMeters().entrySet()) {
            result.addAll(meter(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Gauge> entry : registry.getGauges().entrySet()) {
            result.addAll(gauge(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Counter> entry : registry.getCounters().entrySet()) {
            result.addAll(counter(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<MetricName, Histogram> entry : registry.getHistograms().entrySet()) {
            result.addAll(histogram(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return emptyList();
    }
}
