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

import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.SELECT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.ZERO_BUF;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.RDB_OPCODE_FUNCTION;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static java.nio.ByteBuffer.wrap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.io.LayeredOutputStream;
import com.moilioncircle.redis.rdb.cli.net.protocol.Protocols;
import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.ByteBufferOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpFunction;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * @author Baoyi Chen
 */
public class DumpRdbVisitor extends AbstractRctRdbVisitor {
    
    // TODO https://github.com/leonchen83/redis-rdb-cli/issues/6
    private final boolean replace;
    
    public DumpRdbVisitor(Replicator replicator, Configure configure, Filter filter, File output, boolean replace, Escaper escaper) {
        super(replicator, configure, filter, output, escaper);
        this.replace = replace;
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        long dbnum = db.getDbNumber();
        Protocols.emit(this.out, SELECT, String.valueOf(dbnum).getBytes());
        return db;
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        version = getVersion(version);
        if (version < 10 /* since redis rdb version 10 */) {
            return super.applyFunction(in, version);
        } else {
            try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                    listener.write((byte) RDB_OPCODE_FUNCTION);
                    super.applyFunction(in, version);
                }
                Protocols.functionRestore(this.out, out.toByteBuffers(), replace);
                return new DumpFunction();
            }
        }
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyString(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyString(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyList(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyList(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplySet(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplySet(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSet(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyZSet(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSet2(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            if (version < 8 /* since redis rdb version 8 */) {
                // downgrade to RDB_TYPE_ZSET
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
                    super.doApplyZSet2(in, version, key, type, context);
                }
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHash(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyHash(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashZipMap(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyHashZipMap(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListZipList(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyListZipList(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplySetIntSet(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplySetIntSet(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSetZipList(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyZSetZipList(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSetListPack(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            if (version < 10 /* since redis rdb version 10 */) {
                // downgrade to RDB_TYPE_ZSET
                BaseRdbParser parser = new BaseRdbParser(in);
                BaseRdbEncoder encoder = new BaseRdbEncoder();
                ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
                RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
                listPack.skip(4); // total-bytes
                int len = listPack.readInt(2);
                long targetLen = len / 2;
                while (len > 0) {
                    byte[] element = listPackEntry(listPack);
                    encoder.rdbGenericSaveStringObject(new ByteArray(element), out1);
                    len--;
                    double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
                    encoder.rdbSaveDoubleValue(score, out1);
                    len--;
                }
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper, false)) {
                    listener.write((byte) RDB_TYPE_ZSET);
                    listener.handle(encoder.rdbSaveLen(targetLen));
                    listener.handle(out1.toByteBuffer());
                }
            } else {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                    listener.write((byte) type);
                    super.doApplyZSetListPack(in, version, key, type, context);
                }
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashZipList(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyHashZipList(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashListPack(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            if (version < 10 /* since redis rdb version 10 */) {
                // downgrade to RDB_TYPE_HASH
                BaseRdbParser parser = new BaseRdbParser(in);
                BaseRdbEncoder encoder = new BaseRdbEncoder();
                ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
                RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
                listPack.skip(4); // total-bytes
                int len = listPack.readInt(2);
                long targetLen = len / 2;
                while (len > 0) {
                    byte[] field = listPackEntry(listPack);
                    encoder.rdbGenericSaveStringObject(new ByteArray(field), out1);
                    len--;
                    byte[] value = listPackEntry(listPack);
                    encoder.rdbGenericSaveStringObject(new ByteArray(value), out1);
                    len--;
                }
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper, false)) {
                    listener.write((byte) RDB_TYPE_HASH);
                    listener.handle(encoder.rdbSaveLen(targetLen));
                    listener.handle(out1.toByteBuffer());
                }
            } else {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                    listener.write((byte) type);
                    super.doApplyHashListPack(in, version, key, type, context);
                }
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListQuickList(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
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
                    super.doApplyListQuickList(in, version, key, type, context);
                }
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListQuickList2(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            if (version < 10 /* since redis rdb version 10 */) {
                // downgrade to RDB_TYPE_LIST
                BaseRdbParser parser = new BaseRdbParser(in);
                BaseRdbEncoder encoder = new BaseRdbEncoder();
                ByteBufferOutputStream out1 = new ByteBufferOutputStream(configure.getOutputBufferSize());
                int total = 0;
                long len = parser.rdbLoadLen().len;
                for (long i = 0; i < len; i++) {
                    long container = parser.rdbLoadLen().len;
                    ByteArray bytes = parser.rdbLoadPlainStringObject();
                    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                        encoder.rdbGenericSaveStringObject(new ByteArray(bytes.first()), out1);
                        total++;
                    } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                        RedisInputStream listPack = new RedisInputStream(bytes);
                        listPack.skip(4); // total-bytes
                        int innerLen = listPack.readInt(2);
                        for (int j = 0; j < innerLen; j++) {
                            byte[] e = listPackEntry(listPack);
                            encoder.rdbGenericSaveStringObject(new ByteArray(e), out1);
                            total++;
                        }
                        int lpend = listPack.read(); // lp-end
                        if (lpend != 255) {
                            throw new AssertionError("listpack expect 255 but " + lpend);
                        }
                    } else {
                        throw new UnsupportedOperationException(String.valueOf(container));
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
                    super.doApplyListQuickList2(in, version, key, type, context);
                }
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyModule(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule2(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyModule2(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyStreamListPacks(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                listener.write((byte) type);
                super.doApplyStreamListPacks(in, version, key, type, context);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyStreamListPacks2(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                if (version < 10) {
                    listener.write((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
                } else {
                    listener.write((byte) type);
                }
                super.doApplyStreamListPacks2(in, version, key, type, context, listener);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
}
