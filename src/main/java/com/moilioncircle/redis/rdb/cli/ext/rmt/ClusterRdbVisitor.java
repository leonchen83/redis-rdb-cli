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
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.FUNCTION;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.REPLACE;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.RESTORE;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.RESTORE_ASKING;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ZERO;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.ENDPOINT_FAILURE;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.api.sink.cmd.ClosedCommand;
import com.moilioncircle.redis.rdb.cli.api.sink.cmd.ClosingCommand;
import com.moilioncircle.redis.rdb.cli.api.sink.listener.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoints;
import com.moilioncircle.redis.rdb.cli.util.XThreadFactory;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpFunction;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class ClusterRdbVisitor extends AbstractRmtRdbVisitor implements EventListener {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRdbVisitor.class);
    private static final Monitor MONITOR = MonitorFactory.getMonitor("endpoint");

    private final List<String> lines;
    private final Configuration configuration;
    private ThreadLocal<XEndpoints> endpoints = new ThreadLocal<>();
    
    //noinspection ThisEscapedInObjectConstruction
    public ClusterRdbVisitor(Replicator replicator, Configure configure, Filter filter, RedisURI uri, List<String> lines, boolean replace) throws IOException {
        super(replicator, configure, filter, replace);
        this.lines = lines;
        this.configuration = configure.merge(uri, false);
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure.getMigrateThreads(), new XThreadFactory("sync-worker")));
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        try {
            if (event instanceof PreRdbSyncEvent) {
                XEndpoints prev = this.endpoints.get();
                XEndpoints.closeQuietly(prev);
                List<String> nodes = prev != null ? prev.getClusterNodes() : lines;
                int pipe = configure.getMigrateBatchSize();
                try {
                    this.endpoints.set(new XEndpoints(nodes, pipe, true, configuration));
                } catch (Throwable e) {
                    // unrecoverable error
                    System.out.println("failed to connect cluster nodes, reason : " + e.getMessage());
                    System.exit(-1);
                }
            } else if (event instanceof DumpKeyValuePair) {
                retry((DumpKeyValuePair)event, configure.getMigrateRetries());
            } else if (event instanceof DumpFunction) {
                retry((DumpFunction) event, configure.getMigrateRetries());
            } else if (event instanceof ClosingCommand) {
                this.endpoints.get().flushQuietly();
                XEndpoints.closeQuietly(this.endpoints.get());
            } else if (event instanceof ClosedCommand) {
                MonitorManager.closeQuietly(manager);
            }
        } catch (Throwable e) {
            // should not reach here, but if reach here ,please report an issue
            logger.error("report an issue with exception stack on https://github.com/leonchen83/redis-rdb-cli/issues", e);
            System.out.println("fatal error, check log and report an issue with exception stack.");
            System.exit(-1);
        }
    }

    public void retry(DumpKeyValuePair dkv, int times) {
        logger.trace("sync rdb event [{}], times {}", new String(dkv.getKey()), times);
        short slot = slot(dkv.getKey());
        try {
            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) {
                    MONITOR.add(ENDPOINT_FAILURE, "expired", 1);
                    logger.error("failure[expired] [{}]", new String(dkv.getKey()));
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
                MONITOR.add(ENDPOINT_FAILURE, "failed", 1);
                logger.error("failure[failed] [{}], reason: {}", new String(dkv.getKey()), e.getMessage());
            }
        }
    }
    
    public void retry(DumpFunction dfn, int times) {
        logger.trace("sync rdb event [function], times {}", times);
        try {
            boolean result = false;
            if (!replace) {
                result = endpoints.get().broadcast(FUNCTION, RESTORE, dfn.getSerialized());
            } else {
                result = endpoints.get().broadcast(FUNCTION, RESTORE, dfn.getSerialized(), REPLACE);
            }
            if (!result) throw new RuntimeException("failover");
        } catch (Throwable e) {
            times--;
            if (times >= 0) {
                retry(dfn, times);
            } else {
                MONITOR.add(ENDPOINT_FAILURE, "failed", 1);
                logger.error("failure[failed] [function], reason: {}", e.getMessage());
            }
        }
    }
}
