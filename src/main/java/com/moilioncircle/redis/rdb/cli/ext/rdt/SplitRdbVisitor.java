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

package com.moilioncircle.redis.rdb.cli.ext.rdt;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.Guard;
import com.moilioncircle.redis.rdb.cli.io.ShardableFileOutputStream;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

/**
 * @author Baoyi Chen
 */
public class SplitRdbVisitor extends AbstractRdtRdbVisitor {

    public SplitRdbVisitor(Replicator replicator, Configure configure, Args.RdtArgs arg, Supplier<OutputStream> supplier) {
        super(replicator, configure, arg.filter, supplier);
        this.replicator.addEventListener((rep, event) -> {
            if (event instanceof PreRdbSyncEvent) {
                listener.reset(supplier.get());
            }
            if (event instanceof PostRdbSyncEvent) {
                ShardableFileOutputStream out = listener.getOutputStream();
                out.writeCRC();
                OutputStreams.closeQuietly(out);
            }
            if (event instanceof PreCommandSyncEvent) {
                OutputStreams.closeQuietly(listener.getOutputStream());
            }
        });
    }

    private void shard(byte[] key) {
        ShardableFileOutputStream out = listener.getOutputStream();
        out.shard(key);
    }

    @Override
    public int applyVersion(RedisInputStream in) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyVersion(in);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public Event applyAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyAux(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public Event applyModuleAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyModuleAux(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyFunction(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applySelectDB(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyResizeDB(in, version, context);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyString(in, version, key, type, context);
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyList(in, version, key, type, context);
    }

    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplySet(in, version, key, type, context);
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyZSet(in, version, key, type, context);
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyZSet2(in, version, key, type, context);
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyHash(in, version, key, type, context);
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyHashZipMap(in, version, key, type, context);
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyListZipList(in, version, key, type, context);
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplySetIntSet(in, version, key, type, context);
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyZSetZipList(in, version, key, type, context);
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyHashZipList(in, version, key, type, context);
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyListQuickList(in, version, key, type, context);
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyModule(in, version, key, type, context);
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyModule2(in, version, key, type, context);
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyStreamListPacks(in, version, key, type, context);
    }
    
    @Override
    protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyStreamListPacks2(in, version, key, type, context);
    }

}
