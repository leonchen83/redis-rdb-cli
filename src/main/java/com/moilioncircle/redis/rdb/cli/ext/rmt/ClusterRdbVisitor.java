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

import static com.moilioncircle.redis.rdb.cli.conf.NodeConfParser.slot;
import static com.moilioncircle.redis.replicator.Configuration.defaultSetting;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractMigrateRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.ext.cmd.ClosedCommand;
import com.moilioncircle.redis.rdb.cli.ext.cmd.ClosingCommand;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoints;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class ClusterRdbVisitor extends AbstractMigrateRdbVisitor implements EventListener {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRdbVisitor.class);
    private static final Monitor monitor = MonitorFactory.getMonitor("endpoint_statistics");

    private final List<String> lines;
    private final Configuration configuration;
    private ThreadLocal<XEndpoints> endpoints = new ThreadLocal<>();
    
    public ClusterRdbVisitor(Replicator replicator,
                             Configure configure,
                             List<String> lines,
                             List<String> regexs,
                             List<DataType> types,
                             boolean replace) throws IOException {
        super(replicator, configure, singletonList(0L), regexs, types, replace);
        this.lines = lines;
        this.configuration = configure.merge(defaultSetting());
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure));
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            XEndpoints prev = this.endpoints.get();
            XEndpoints.closeQuietly(prev);
            List<String> nodes = prev != null ? prev.getClusterNodes() : lines;
            int pipe = configure.getMigrateBatchSize();
            try {
                this.endpoints.set(new XEndpoints(nodes, pipe, true, configuration, configure));
            } catch (Throwable e) {
                // unrecoverable error
                System.out.println("failed to connect cluster nodes, reason : " + e.getMessage());
                System.exit(-1);
            }
        } else if (event instanceof DumpKeyValuePair) {
            retry((DumpKeyValuePair)event, configure.getMigrateRetries());
        } else if (event instanceof ClosingCommand) {
            this.endpoints.get().flushQuietly();
            XEndpoints.closeQuietly(this.endpoints.get());
        } else if (event instanceof ClosedCommand) {
            MonitorManager.closeQuietly(manager);
        }
    }

    public void retry(DumpKeyValuePair dkv, int times) {
        short slot = slot(dkv.getKey());
        try {
            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) {
                    monitor.add("failure_expired", 1);
                    logger.debug("failure[expired] [{}]", new String(dkv.getKey()));
                    return;
                }
                expire = String.valueOf(ms).getBytes();
            }

            if (!replace) {
                endpoints.get().batch(flush, slot, RESTORE_ASKING, dkv.getKey(), expire, dkv.getValue());
            } else {
                // https://github.com/leonchen83/redis-rdb-cli/issues/6 --no need to use lua script
                endpoints.get().batch(flush, slot, RESTORE_ASKING, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                this.endpoints.get().updateQuietly(slot);
                retry(dkv, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}], reason: {}", new String(dkv.getKey()), e.getMessage());
            }
        }
    }
}
