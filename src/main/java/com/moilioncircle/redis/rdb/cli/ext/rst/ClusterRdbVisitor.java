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

import static com.moilioncircle.redis.rdb.cli.conf.NodeConfParser.slot;
import static com.moilioncircle.redis.replicator.Configuration.defaultSetting;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractMigrateRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.ext.cmd.ClosedCommand;
import com.moilioncircle.redis.rdb.cli.ext.cmd.ClosingCommand;
import com.moilioncircle.redis.rdb.cli.ext.cmd.CombineCommand;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoints;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.impl.BRPopLPushCommand;
import com.moilioncircle.redis.replicator.cmd.impl.BitOpCommand;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.impl.DelCommand;
import com.moilioncircle.redis.replicator.cmd.impl.GenericKeyCommand;
import com.moilioncircle.redis.replicator.cmd.impl.MSetCommand;
import com.moilioncircle.redis.replicator.cmd.impl.MSetNxCommand;
import com.moilioncircle.redis.replicator.cmd.impl.PFCountCommand;
import com.moilioncircle.redis.replicator.cmd.impl.PFMergeCommand;
import com.moilioncircle.redis.replicator.cmd.impl.RPopLPushCommand;
import com.moilioncircle.redis.replicator.cmd.impl.RenameCommand;
import com.moilioncircle.redis.replicator.cmd.impl.RenameNxCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SDiffStoreCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SInterStoreCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SMoveCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SelectCommand;
import com.moilioncircle.redis.replicator.cmd.impl.UnLinkCommand;
import com.moilioncircle.redis.replicator.cmd.impl.ZInterStoreCommand;
import com.moilioncircle.redis.replicator.cmd.impl.ZUnionStoreCommand;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class ClusterRdbVisitor extends AbstractMigrateRdbVisitor implements EventListener {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRdbVisitor.class);
    private static final Monitor monitor = MonitorFactory.getMonitor("endpoint_statistics");

    private int db;
    private final List<String> lines;
    private final Configuration configuration;
    private ThreadLocal<XEndpoints> endpoints = new ThreadLocal<>();
    
    public ClusterRdbVisitor(Replicator replicator,
                             Configure configure,
                             List<String> lines,
                             boolean replace) throws IOException {
        super(replicator, configure, singletonList(0L), new ArrayList<>(), new ArrayList<>(), replace);
        this.lines = lines;
        this.configuration = configure.merge(defaultSetting());
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure));
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
        } else if (event instanceof PostRdbSyncEvent) {
            this.endpoints.get().flushQuietly();
        } else if (event instanceof PreCommandSyncEvent) {
            this.endpoints.get().flushQuietly();
        } else if (event instanceof SelectCommand) {
            SelectCommand select = (SelectCommand)event;
            this.db = select.getIndex();
        } else if (event instanceof CombineCommand) {
            if (containsDB(db)) {
                retry((CombineCommand)event, configure.getMigrateRetries());
            }
        } else if (event instanceof ClosingCommand) {
            this.endpoints.get().flushQuietly();
            XEndpoints.closeQuietly(this.endpoints.get());
            MonitorManager.closeQuietly(manager);
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
    
    public void retry(CombineCommand command, short slot, int times) {
        try {
            DefaultCommand dcmd = command.getDefaultCommand();
            endpoints.get().batch(flush, slot, dcmd.getCommand(), dcmd.getArgs());
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                this.endpoints.get().updateQuietly(slot);
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}], reason: {}", command, e.getMessage());
            }
        }
    }

    public void retry(CombineCommand command, int times) {
        Command parsedCommand = command.getParsedCommand();
        if (parsedCommand instanceof RenameCommand) {
            RenameCommand cmd = (RenameCommand) parsedCommand;
            short slot = slot1(cmd.getKey(), cmd.getNewKey());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof RenameNxCommand) {
            RenameNxCommand cmd = (RenameNxCommand) parsedCommand;
            short slot = slot1(cmd.getKey(), cmd.getNewKey());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof PFMergeCommand) {
            PFMergeCommand cmd = (PFMergeCommand) parsedCommand;
            short slot = slot1(cmd.getDestkey(), cmd.getSourcekeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof PFCountCommand) {
            PFCountCommand cmd = (PFCountCommand) parsedCommand;
            short slot = slot0(cmd.getKeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof MSetNxCommand) {
            MSetNxCommand cmd = (MSetNxCommand) parsedCommand;
            byte[][] keys = cmd.getKv().keySet().toArray(new byte[0][]);
            short slot = slot0(keys);
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof BRPopLPushCommand) {
            BRPopLPushCommand cmd = (BRPopLPushCommand) parsedCommand;
            short slot = slot0(cmd.getDestination(), cmd.getSource());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof BitOpCommand) {
            BitOpCommand cmd = (BitOpCommand) parsedCommand;
            short slot = slot1(cmd.getDestkey(), cmd.getKeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof MSetCommand) {
            MSetCommand cmd = (MSetCommand) parsedCommand;
            byte[][] keys = cmd.getKv().keySet().toArray(new byte[0][]);
            short slot = slot0(keys);
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof UnLinkCommand) {
            UnLinkCommand cmd = (UnLinkCommand) parsedCommand;
            short slot = slot0(cmd.getKeys());
            if (slot != -1) {
                retry(command, slot(cmd.getKeys()[0]), times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof DelCommand) {
            DelCommand cmd = (DelCommand) parsedCommand;
            short slot = slot0(cmd.getKeys());
            if (slot != -1) {
                retry(command, slot(cmd.getKeys()[0]), times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof ZUnionStoreCommand) {
            ZUnionStoreCommand cmd = (ZUnionStoreCommand) parsedCommand;
            short slot = slot1(cmd.getDestination(), cmd.getKeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof ZInterStoreCommand) {
            ZInterStoreCommand cmd = (ZInterStoreCommand) parsedCommand;
            short slot = slot1(cmd.getDestination(), cmd.getKeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof SMoveCommand) {
            SMoveCommand cmd = (SMoveCommand) parsedCommand;
            short slot = slot1(cmd.getDestination(), cmd.getSource());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof SInterStoreCommand) {
            SInterStoreCommand cmd = (SInterStoreCommand) parsedCommand;
            short slot = slot1(cmd.getDestination(), cmd.getKeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof SDiffStoreCommand) {
            SDiffStoreCommand cmd = (SDiffStoreCommand) parsedCommand;
            short slot = slot1(cmd.getDestination(), cmd.getKeys());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof RPopLPushCommand) {
            RPopLPushCommand cmd = (RPopLPushCommand) parsedCommand;
            short slot = slot1(cmd.getDestination(), cmd.getSource());
            if (slot != -1) {
                retry(command, slot, times);
            } else {
                monitor.add("failure_failed", 1);
                logger.error("failure[failed] [{}]", command);
            }
        } else if (parsedCommand instanceof GenericKeyCommand) {
            GenericKeyCommand cmd = (GenericKeyCommand) parsedCommand;
            retry(command, slot(cmd.getKey()), times);
        } else {
            // swapdb
            // move
            // flushall
            // flushdb
            // publish
            // multi
            // exec
            // script flush
            // script load
            // eval
            // evalsha
            monitor.add("failure_unsupported", 1);
            logger.error("failure[unsupported] [{}]", command);
        }
    }

    public static short slot0(byte[]... keys) {
        short slot = slot(keys[0]);
        for (int i = 1; i < keys.length; i++) {
            if (slot != slot(keys[i])) return -1;
        }
        return slot;
    }

    public static short slot1(byte[] key, byte[]... keys) {
        short slot = slot(key);
        for (int i = 0; i < keys.length; i++) {
            if (slot != slot(keys[i])) return -1;
        }
        return slot;
    }
}
