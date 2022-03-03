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

package com.moilioncircle.redis.rdb.cli.ext.rct;

import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.FUNCTION;
import static com.moilioncircle.redis.replicator.Constants.RDB_OPCODE_FUNCTION;
import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.NONE;

import java.io.File;
import java.io.IOException;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;

/**
 * @author Baoyi Chen
 */
public class DiffRdbVisitor extends AbstractRdbVisitor {
    
    public DiffRdbVisitor(Replicator replicator, Configure configure, File out, Filter filter, Escaper escaper) {
        super(replicator, configure, out, filter, escaper);
    }
    
    protected void expire(ExpiredType type, Long value) {
        if (type != NONE) {
            escaper.encode(type.toString().getBytes(), out);
            delimiter(out);
            escaper.encode(value, out);
            delimiter(out);
        }
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        escaper.encode(FUNCTION, out);
        delimiter(out);
        Event event = null;
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) RDB_OPCODE_FUNCTION);
            event = super.applyFunction(in, version);
        }
        OutputStreams.write('\n', out);
        return event;
    }
    
    @Override
    public Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyString(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyList(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplySet(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyZSet(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyZSet2(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyHash(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyHashZipMap(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyListZipList(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplySetIntSet(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyZSetZipList(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyZSetListPack(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyHashZipList(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyHashListPack(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyListQuickList(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyListQuickList2(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyModule(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyModule2(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            listener.write((byte) type);
            super.doApplyStreamListPacks(in, version, key, contains, type, context);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escaper.encode(key, out);
        delimiter(out);
        expire(context.getExpiredType(), context.getExpiredValue());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
            if (version < 10) {
                listener.write((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
            } else {
                listener.write((byte) type);
            }
            super.doApplyStreamListPacks2(in, version, key, contains, type, context, listener);
        }
        OutputStreams.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
}
