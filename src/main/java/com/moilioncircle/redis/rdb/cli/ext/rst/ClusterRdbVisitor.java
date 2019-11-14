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

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractMigrateRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.ext.CloseEvent;
import com.moilioncircle.redis.rdb.cli.ext.rst.cmd.CombineCommand;
import com.moilioncircle.redis.rdb.cli.net.Endpoints;
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
import com.moilioncircle.redis.replicator.cmd.impl.MoveCommand;
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

    private int db;
    private final List<String> lines;
    private final Configuration configuration;
    private ThreadLocal<Endpoints> endpoints = new ThreadLocal<>();
    
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
            Endpoints.closeQuietly(this.endpoints.get());
            int pipe = configure.getMigrateBatchSize();
            this.endpoints.set(new Endpoints(lines, pipe, true, configuration, configure));
        } else if (event instanceof DumpKeyValuePair) {
            retry((DumpKeyValuePair)event, configure.getMigrateRetries());
        } else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            this.endpoints.get().flush();
        } else if (event instanceof SelectCommand) {
            SelectCommand select = (SelectCommand)event;
            this.db = select.getIndex();
        } else if (event instanceof CombineCommand) {
            if (containsDB(db)) {
                retry((CombineCommand)event, configure.getMigrateRetries());
            }
        } else if (event instanceof CloseEvent) {
            this.endpoints.get().flush();
            Endpoints.closeQuietly(this.endpoints.get());
        }
    }

    public void retry(DumpKeyValuePair dkv, int times) {
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
                retry(dkv, times);
            }
        }
    }
    
    public void retry(CombineCommand command, byte[] key, int times) {
        try {
            DefaultCommand dcmd = command.getDefaultCommand();
            endpoints.get().batchCommand(flush, key, dcmd.getCommand(), dcmd.getArgs());
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                this.endpoints.get().update(key);
                retry(command, times);
            }
        }
    }

    public void retry(CombineCommand command, int times) {
        Command parsedCommand = command.getParsedCommand();
        if (parsedCommand instanceof RenameCommand) {
            RenameCommand cmd = (RenameCommand) parsedCommand;
            if (isSameSlot1(cmd.getKey(), cmd.getNewKey())) {
                retry(command, cmd.getKey(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof RenameNxCommand) {
            RenameNxCommand cmd = (RenameNxCommand) parsedCommand;
            if (isSameSlot1(cmd.getKey(), cmd.getNewKey())) {
                retry(command, cmd.getKey(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof PFMergeCommand) {
            PFMergeCommand cmd = (PFMergeCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestkey(), cmd.getSourcekeys())) {
                retry(command, cmd.getDestkey(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof PFCountCommand) {
            PFCountCommand cmd = (PFCountCommand) parsedCommand;
            if (isSameSlot0(cmd.getKeys())) {
                retry(command, cmd.getKeys()[0], times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof MSetNxCommand) {
            MSetNxCommand cmd = (MSetNxCommand) parsedCommand;
            byte[][] keys = cmd.getKv().keySet().toArray(new byte[0][]);
            if (isSameSlot0(keys)) {
                retry(command, keys[0], times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof BRPopLPushCommand) {
            BRPopLPushCommand cmd = (BRPopLPushCommand) parsedCommand;
            if (isSameSlot0(cmd.getDestination(), cmd.getSource())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof MoveCommand) {
            MoveCommand cmd = (MoveCommand) parsedCommand;
            if (cmd.getDb() == 0) {
                retry(command, cmd.getKey(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof BitOpCommand) {
            BitOpCommand cmd = (BitOpCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestkey(), cmd.getKeys())) {
                retry(command, cmd.getDestkey(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof MSetCommand) {
            MSetCommand cmd = (MSetCommand) parsedCommand;
            byte[][] keys = cmd.getKv().keySet().toArray(new byte[0][]);
            if (isSameSlot0(keys)) {
                retry(command, keys[0], times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof UnLinkCommand) {
            UnLinkCommand cmd = (UnLinkCommand) parsedCommand; 
            if (cmd.getKeys().length == 1) {
                retry(command, cmd.getKeys()[0], times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof DelCommand) {
            DelCommand cmd = (DelCommand) parsedCommand;
            if (cmd.getKeys().length == 1) {
                retry(command, cmd.getKeys()[0], times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof ZUnionStoreCommand) {
            ZUnionStoreCommand cmd = (ZUnionStoreCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestination(), cmd.getKeys())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof ZInterStoreCommand) {
            ZInterStoreCommand cmd = (ZInterStoreCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestination(), cmd.getKeys())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof SMoveCommand) {
            SMoveCommand cmd = (SMoveCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestination(), cmd.getSource())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof SInterStoreCommand) {
            SInterStoreCommand cmd = (SInterStoreCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestination(), cmd.getKeys())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof SDiffStoreCommand) {
            SDiffStoreCommand cmd = (SDiffStoreCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestination(), cmd.getKeys())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof RPopLPushCommand) {
            RPopLPushCommand cmd = (RPopLPushCommand) parsedCommand;
            if (isSameSlot1(cmd.getDestination(), cmd.getSource())) {
                retry(command, cmd.getDestination(), times);
            } else {
                logger.error("failed to sync command [{}]", command);
            }
        } else if (parsedCommand instanceof GenericKeyCommand) {
            GenericKeyCommand cmd = (GenericKeyCommand) parsedCommand;
            retry(command, cmd.getKey(), times);
        } else {
            // swapdb
            // flushall
            // flushdb
            // script flush
            // script load
            // publish
            // multi
            // exec
            // eval
            // evalsha
            logger.error("unsupported to sync command [{}]", command);
        }
    }

    public static boolean isSameSlot0(byte[]... keys) {
        int slot = slot(keys[0]);
        for (int i = 1; i < keys.length; i++) {
            if (slot != slot(keys[i])) return false;
        }
        return true;
    }

    public static boolean isSameSlot1(byte[] key, byte[]... keys) {
        int slot = slot(key);
        for (int i = 0; i < keys.length; i++) {
            if (slot != slot(keys[i])) return false;
        }
        return true;
    }
}
