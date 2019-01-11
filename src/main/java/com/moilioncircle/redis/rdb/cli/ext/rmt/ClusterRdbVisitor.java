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

package com.moilioncircle.redis.rdb.cli.ext.rmt;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.metric.MetricJobs;
import com.moilioncircle.redis.rdb.cli.net.Endpoints;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.moilioncircle.redis.rdb.cli.metric.MetricReporterFactory.create;
import static com.moilioncircle.redis.replicator.Configuration.defaultSetting;
import static java.util.Collections.singletonList;

/**
 * @author Baoyi Chen
 */
public class ClusterRdbVisitor extends AbstractMigrateRdbVisitor implements EventListener {

    private final List<String> lines;
    private final Configuration configuration;
    private ThreadLocal<Endpoints> endpoints = new ThreadLocal<>();

    public ClusterRdbVisitor(Replicator replicator, Configure configure, List<String> lines, List<String> regexs, List<DataType> types, boolean replace) throws IOException {
        super(replicator, configure, singletonList(0L), regexs, types, replace);
        this.lines = lines;
        this.configuration = configure.merge(defaultSetting());
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure));
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            Endpoints.closeQuietly(this.endpoints.get());
            int pipe = configure.getMigrateBatchSize();
            this.endpoints.set(new Endpoints(lines, pipe, registry, configuration));

            if (this.reporter != null) this.reporter.close();
            this.reporter = create(configure, registry, MetricJobs.endpoint(configure));
            this.reporter.start(5, TimeUnit.SECONDS);
        } else if (event instanceof DumpKeyValuePair) {
            retry(event, configure.getMigrateRetries());
        } else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            this.endpoints.get().flush();
            Endpoints.closeQuietly(this.endpoints.get());

            if (this.reporter != null) {
                this.reporter.report();
                this.reporter.close();
            }
        }
    }

    public void retry(Event event, int times) {
        DumpKeyValuePair dkv = (DumpKeyValuePair) event;
        try {
            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) return;
                expire = String.valueOf(ms).getBytes();
            }

            if (!replace) {
                endpoints.get().batch(flush, RESTORE_ASKING, dkv.getKey(), expire, dkv.getValue());
            } else {
                // https://github.com/leonchen83/redis-rdb-cli/issues/6 --no need to use lua script
                endpoints.get().batch(flush, RESTORE_ASKING, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                this.endpoints.get().update(dkv.getKey());
                retry(event, times);
            }
        }
    }
}
