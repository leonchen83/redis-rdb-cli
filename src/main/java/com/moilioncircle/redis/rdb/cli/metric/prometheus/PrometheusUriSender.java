package com.moilioncircle.redis.rdb.cli.metric.prometheus;

import com.moilioncircle.redis.rdb.cli.metric.MetricConfigure;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * @author Baoyi Chen
 */
public class PrometheusUriSender implements PrometheusSender {

    private PushGateway gateway;

    public PrometheusUriSender(MetricConfigure configure) {
        URI uri = configure.getUri();
        try {
            this.gateway = new PushGateway(uri.toURL());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void push(CollectorRegistry registry, String job) throws IOException {
        gateway.push(registry, job);
    }

    @Override
    public void push(Collector collector, String job) throws IOException {
        gateway.push(collector, job);
    }

    @Override
    public void push(CollectorRegistry registry, String job, Map<String, String> groupingKey) throws IOException {
        gateway.push(registry, job, groupingKey);
    }

    @Override
    public void push(Collector collector, String job, Map<String, String> groupingKey) throws IOException {
        gateway.push(collector, job, groupingKey);
    }

    @Override
    public void pushAdd(CollectorRegistry registry, String job) throws IOException {
        gateway.pushAdd(registry, job);
    }

    @Override
    public void pushAdd(Collector collector, String job) throws IOException {
        gateway.pushAdd(collector, job);
    }

    @Override
    public void pushAdd(CollectorRegistry registry, String job, Map<String, String> groupingKey) throws IOException {
        gateway.pushAdd(registry, job, groupingKey);
    }

    @Override
    public void pushAdd(Collector collector, String job, Map<String, String> groupingKey) throws IOException {
        gateway.pushAdd(collector, job, groupingKey);
    }

    @Override
    public void delete(String job) throws IOException {
        gateway.delete(job);
    }

    @Override
    public void delete(String job, Map<String, String> groupingKey) throws IOException {
        gateway.delete(job, groupingKey);
    }
}
