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

import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.REPLACE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.RESTORE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.SELECT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.ZERO_BUF;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.STAR;
import static java.nio.ByteBuffer.wrap;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.escape.RawEscaper;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.io.LayeredOutputStream;
import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.ByteBufferOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.util.ByteArray;

/**
 * @author Baoyi Chen
 */
public class DumpRdbVisitor extends AbstractRdbVisitor {
    
    // TODO https://github.com/leonchen83/redis-rdb-cli/issues/6
    private final boolean replace;
    
    public DumpRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) {
        super(replicator, configure, out, db, regexs, types, new RawEscaper());
        this.replace = replace;
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        long dbnum = db.getDbNumber();
        emit(this.out, SELECT, String.valueOf(dbnum).getBytes());
        return db;
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyString(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyString(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyList(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyList(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplySet(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplySet(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSet(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyZSet(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSet2(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            if (version < 8 /* since redis rdb version 8 */) {
                // downgrade to RDB_TYPE_LIST
                BaseRdbParser parser = new BaseRdbParser(in);
                BaseRdbEncoder encoder = new BaseRdbEncoder();
                ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
                long len = parser.rdbLoadLen().len;
                long temp = len;
                while (len > 0) {
                    ByteArray element = parser.rdbLoadEncodedStringObject();
                    encoder.rdbGenericSaveStringObject(element, out1);
                    double score = parser.rdbLoadBinaryDoubleValue();
                    encoder.rdbSaveDoubleValue(score, out1);
                    len--;
                }
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper, false)) {
                    listener.write((byte) RDB_TYPE_ZSET);
                    listener.handle(encoder.rdbSaveLen(temp));
                    listener.handle(out1.toByteBuffer());
                }
            } else {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                    listener.write((byte) type);
                    super.doApplyZSet2(in, version, key, contains, type, context);
                }
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHash(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyHash(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashZipMap(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyHashZipMap(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListZipList(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyListZipList(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplySetIntSet(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplySetIntSet(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSetZipList(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyZSetZipList(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashZipList(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyHashZipList(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListQuickList(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            if (version < 7 /* since redis rdb version 7 */) {
                BaseRdbParser parser = new BaseRdbParser(in);
                BaseRdbEncoder encoder = new BaseRdbEncoder();
                ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
                int total = 0;
                long len = parser.rdbLoadLen().len;
                for (long i = 0; i < len; i++) {
                    RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
        
                    BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                    BaseRdbParser.LenHelper.zltail(stream); // zltail
                    int zllen = BaseRdbParser.LenHelper.zllen(stream);
                    for (int j = 0; j < zllen; j++) {
                        byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                        encoder.rdbGenericSaveStringObject(new ByteArray(e), out1);
                        total++;
                    }
                    int zlend = BaseRdbParser.LenHelper.zlend(stream);
                    if (zlend != 255) {
                        throw new AssertionError("zlend expect 255 but " + zlend);
                    }
                }
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper, false)) {
                    listener.write((byte) RDB_TYPE_LIST);
                    listener.handle(encoder.rdbSaveLen(total));
                    listener.handle(out1.toByteBuffer());
                }
            } else {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                    listener.write((byte) type);
                    super.doApplyListQuickList(in, version, key, contains, type, context);
                }
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyModule(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule2(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyModule2(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyStreamListPacks(in, version, key, contains, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
            }
            emit(this.out, RESTORE_BUF, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    protected void emit(OutputStream out, ByteBuffer command, ByteBuffer key, ByteBuffer ex, ByteBuffers value, boolean replace) {
        OutputStreams.write(STAR, out);
        if (replace) {
            OutputStreams.write("5".getBytes(), out);
        } else {
            OutputStreams.write("4".getBytes(), out);
        }
        
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        
        emitArg(command);
        emitArg(key);
        emitArg(ex);
        emitArg(value);
        if (replace) {
            emitArg(REPLACE_BUF);
        }
    }
    
    protected void emitArg(ByteBuffer arg) {
        OutputStreams.write(DOLLAR, out);
        OutputStreams.write(String.valueOf(arg.remaining()).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(arg.array(), arg.position(), arg.limit(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
    }
    
    protected void emitArg(ByteBuffers value) {
        OutputStreams.write(DOLLAR, out);
        OutputStreams.write(String.valueOf(value.getSize()).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        while (value.getBuffers().hasNext()) {
            ByteBuffer buf = value.getBuffers().next();
            OutputStreams.write(buf.array(), buf.position(), buf.limit(), out);
        }
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
    }
}
