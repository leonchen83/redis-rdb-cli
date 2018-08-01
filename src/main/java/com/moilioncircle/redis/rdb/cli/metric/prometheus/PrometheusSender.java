package com.moilioncircle.redis.rdb.cli.metric.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.io.IOException;
import java.util.Map;

/**
 * @author Baoyi Chen
 */
public interface PrometheusSender {

    void push(CollectorRegistry registry, String job) throws IOException;

    void push(Collector collector, String job) throws IOException;

    void push(CollectorRegistry registry, String job, Map<String, String> groupingKey) throws IOException;

    void push(Collector collector, String job, Map<String, String> groupingKey) throws IOException;

    void pushAdd(CollectorRegistry registry, String job) throws IOException;

    void pushAdd(Collector collector, String job) throws IOException;

    void pushAdd(CollectorRegistry registry, String job, Map<String, String> groupingKey) throws IOException;

    void pushAdd(Collector collector, String job, Map<String, String> groupingKey) throws IOException;

    void delete(String job) throws IOException;

    void delete(String job, Map<String, String> groupingKey) throws IOException;
}
