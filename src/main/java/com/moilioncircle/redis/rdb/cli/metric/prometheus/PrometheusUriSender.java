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

package com.moilioncircle.redis.rdb.cli.metric.prometheus;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
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
    
    public PrometheusUriSender(Configure configure) {
        URI uri = configure.getMetricUri();
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
