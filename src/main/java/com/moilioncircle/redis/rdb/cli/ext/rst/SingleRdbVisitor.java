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

package com.moilioncircle.redis.rdb.cli.ext.rst;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractMigrateRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.cmd.CombineCommand;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.util.XThreadFactory;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.impl.PingCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SelectCommand;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.sink.api.cmd.ClosedCommand;
import com.moilioncircle.redis.sink.api.cmd.ClosingCommand;
import com.moilioncircle.redis.sink.api.listener.AsyncEventListener;

/**
 * @author Baoyi Chen
 */
public class SingleRdbVisitor extends AbstractMigrateRdbVisitor implements EventListener {

    private static final Logger logger = LoggerFactory.getLogger(SingleRdbVisitor.class);
    private static final Monitor monitor = MonitorFactory.getMonitor("endpoint_statistics");

    private int db;
    private long ping = 0;
    private final RedisURI uri;
    private final boolean legacy;
    private volatile byte[] evalSha;
    private final Configuration conf;
    private ThreadLocal<XEndpoint> endpoint = new ThreadLocal<>();
    
    public SingleRdbVisitor(Replicator replicator,
                            Configure configure,
                            RedisURI uri,
                            List<Long> db,
                            boolean replace,
                            boolean legacy) throws Exception {
        super(replicator, configure, db, new ArrayList<>(), new ArrayList<>(), replace);
        this.uri = uri;
        this.legacy = legacy;
        this.conf = configure.merge(this.uri);
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure.getMigrateThreads(), new XThreadFactory("sync-worker")));
    }

    @Override
    protected boolean containsType(int type) {
        return true;
    }

    @Override
    protected boolean containsKey(String key) {
        return true;
    }
    
    @Override
    public void onEvent(Replicator replicator, Event event) {
        try {
            if (event instanceof PreRdbSyncEvent) {
                XEndpoint.closeQuietly(this.endpoint.get());
                int pipe = configure.getMigrateBatchSize();
                try {
                    this.endpoint.set(new XEndpoint(uri.getHost(), uri.getPort(), 0, pipe, true, conf, configure));
                } catch (Throwable e) {
                    // unrecoverable error
                    System.out.println("failed to connect " + uri.getHost() + ":" + uri.getPort() + ", reason : " + e.getMessage());
                    System.exit(-1);
                }
            } else if (event instanceof DumpKeyValuePair) {
                retry((DumpKeyValuePair)event, configure.getMigrateRetries());
            } else if (event instanceof PostRdbSyncEvent) {
                this.endpoint.get().flushQuietly();
            } else if (event instanceof PreCommandSyncEvent) {
                this.endpoint.get().flushQuietly();
            } else if (event instanceof SelectCommand) {
                SelectCommand select = (SelectCommand)event;
                this.db = select.getIndex();
                if (containsDB(db)) {
                    DefaultCommand command = new DefaultCommand();
                    command.setCommand("SELECT".getBytes());
                    command.setArgs(new byte[][]{String.valueOf(db).getBytes()});
                    retry(command, configure.getMigrateRetries());
                }
            } else if (event instanceof CombineCommand) {
                CombineCommand command = (CombineCommand)event;
                if (command.getParsedCommand() instanceof PingCommand) {
                    
                    if (ping == 0) {
                        ping = System.currentTimeMillis();
                    }
                    // ping every 10s
                    if (System.currentTimeMillis() - ping > 10000) {
                        retry(command.getDefaultCommand(), configure.getMigrateRetries());
                        ping = System.currentTimeMillis();
                    }
                } else if (containsDB(db)) {
                    retry(command.getDefaultCommand(), configure.getMigrateRetries());
                }
            } else if (event instanceof ClosingCommand) {
                this.endpoint.get().flushQuietly();
                XEndpoint.closeQuietly(this.endpoint.get());
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
    
    public void retry(DefaultCommand command, int times) {
        logger.trace("sync aof event [{}], times {}", CombineCommand.toString(command), times);
        try {
            endpoint.get().batch(flush, command.getCommand(), command.getArgs());
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                XEndpoint next = XEndpoint.valueOfQuietly(endpoint.get(), db);
                if (next != null) endpoint.set(next);
                retry(command, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}], reason: {}", CombineCommand.toString(command), e.getMessage());
            }
        }
    }
    
    public void retry(DumpKeyValuePair dkv, int times) {
        logger.trace("sync rdb event [{}], times {}", new String(dkv.getKey()), times);
        try {
            DB db = dkv.getDb();
    
            int index;
            if (db != null && (index = (int) db.getDbNumber()) != endpoint.get().getDB()) {
                endpoint.get().select(true, index);
            }
    
            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) {
                    monitor.add("failure_expired", 1);
                    logger.debug("failure[expired] [{}].", new String(dkv.getKey()));
                    return;
                }
                expire = String.valueOf(ms).getBytes();
            }
            if (!replace) {
                endpoint.get().batch(flush, RESTORE, dkv.getKey(), expire, dkv.getValue());
            } else if (legacy) {
                // https://github.com/leonchen83/redis-rdb-cli/issues/6
                eval(dkv.getKey(), dkv.getValue(), expire);
            } else {
                endpoint.get().batch(flush, RESTORE, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                XEndpoint next = XEndpoint.valueOfQuietly(endpoint.get(), db);
                if (next != null) endpoint.set(next);
                retry(dkv, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}], reason: {}", new String(dkv.getKey()), e.getMessage());
            }
        }
    }
    
    /*
     * LUA
     * step 1 : SCRIPT LOAD script
     * step 2 : EVALSHA sha1
     */
    private static final byte[] LUA_SCRIPT =
            ("redis.call('del',KEYS[1]);return redis.call('restore',KEYS[1],ARGV[1],ARGV[2]);").getBytes();
    
    protected void eval(byte[] key, byte[] value, byte[] expire) {
        if (evalSha == null) {
            RedisObject r = endpoint.get().send(SCRIPT, LOAD, LUA_SCRIPT);
            byte[] evalSha = r.getBytes();
            if (evalSha == null) throw new RuntimeException(); // retry in the caller method.
            this.evalSha = evalSha;
            eval(key, value, expire);
        } else {
            endpoint.get().batch(flush, EVALSHA, evalSha, ONE, key, expire, value);
        }
    }
}
