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

import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.DEL;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.DESCRIPTION_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.EXPIREAT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.FUNCTION_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.HMSET;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.LOAD_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.PEXPIREAT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.REPLACE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.RESTORE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.RPUSH;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.SADD;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.SELECT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.SET;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.ZADD;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.ZERO_BUF;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.STAR;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static java.nio.ByteBuffer.wrap;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Function;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * @author Baoyi Chen
 */
public class RespRdbVisitor extends AbstractRdbVisitor {
    
    private final int batch;
    private final boolean replace;
    
    public RespRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) {
        super(replicator, configure, out, db, regexs, types, new RawEscaper());
        this.batch = configure.getBatchSize();
        this.replace = replace;
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        Function function = (Function) super.applyFunction(in, version);
        emit(this.out, function, replace);
        return function;
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        emit(this.out, SELECT, String.valueOf(db.getDbNumber()).getBytes());
        return db;
    }
    
    @Override
    public Event applyExpireTime(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTime(in, version, context);
        if (!kv.isContains() || kv.getKey() == null) return context.valueOf(kv);
        emit(this.out, EXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredSeconds()).getBytes());
        return context.valueOf(kv);
    }
    
    @Override
    public Event applyExpireTimeMs(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTimeMs(in, version, context);
        if (!kv.isContains() || kv.getKey() == null) return context.valueOf(kv);
        emit(this.out, PEXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredMs()).getBytes());
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        emit(this.out, SET, key, val);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            if (list.size() == batch) {
                emit(this.out, RPUSH, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            if (list.size() == batch) {
                emit(this.out, SADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(this.out, SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                emit(this.out, ZADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                emit(this.out, ZADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                emit(this.out, HMSET, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(this.out, HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        List<byte[]> list = new ArrayList<>();
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                if (!list.isEmpty()) {
                    emit(this.out, HMSET, key, list);
                    list.clear();
                }
                DummyKeyValuePair kv = new DummyKeyValuePair();
                kv.setValueRdbType(type);
                kv.setKey(key);
                kv.setContains(contains);
                return context.valueOf(kv);
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                list.add(field);
                list.add(null);
                emit(this.out, HMSET, key, list);
                list.clear();
                DummyKeyValuePair kv = new DummyKeyValuePair();
                kv.setValueRdbType(type);
                kv.setKey(key);
                kv.setContains(contains);
                return context.valueOf(kv);
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                emit(this.out, HMSET, key, list);
                list = new ArrayList<>();
            }
        }
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < zllen; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            list.add(e);
            if (list.size() == batch) {
                emit(this.out, RPUSH, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!list.isEmpty()) emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        
        List<byte[]> list = new ArrayList<>();
        int encoding = BaseRdbParser.LenHelper.encoding(stream);
        long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
        for (long i = 0; i < lenOfContent; i++) {
            String element;
            switch (encoding) {
                case 2:
                    element = String.valueOf(stream.readInt(2));
                    break;
                case 4:
                    element = String.valueOf(stream.readInt(4));
                    break;
                case 8:
                    element = String.valueOf(stream.readLong(8));
                    break;
                default:
                    throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
            }
            list.add(element.getBytes());
            if (list.size() == batch) {
                emit(this.out, SADD, key, list);
                list.clear();
            }
        }
        if (!list.isEmpty()) emit(this.out, SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        List<byte[]> list = new ArrayList<>();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                emit(this.out, ZADD, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!list.isEmpty()) emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        List<byte[]> list = new ArrayList<>();
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] element = listPackEntry(listPack);
            len--;
            double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
            len--;
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                emit(this.out, ZADD, key, list);
                list.clear();
            }
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        if (!list.isEmpty()) emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        List<byte[]> list = new ArrayList<>();
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                emit(this.out, HMSET, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!list.isEmpty()) emit(this.out, HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] field = listPackEntry(listPack);
            len--;
            byte[] value = listPackEntry(listPack);
            len--;
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                emit(this.out, HMSET, key, list);
                list.clear();
            }
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        if (!list.isEmpty()) emit(this.out, HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> list = new ArrayList<>();
        long len = parser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
            
            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                list.add(e);
                if (list.size() == batch) {
                    emit(this.out, RPUSH, key, list);
                    list.clear();
                }
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        if (!list.isEmpty()) emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (replace) emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> list = new ArrayList<>();
        long len = parser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            long container = parser.rdbLoadLen().len;
            ByteArray bytes = parser.rdbLoadPlainStringObject();
            if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                list.add(bytes.first());
                if (list.size() == batch) {
                    emit(this.out, RPUSH, key, list);
                    list.clear();
                }
            } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                RedisInputStream listPack = new RedisInputStream(bytes);
                listPack.skip(4); // total-bytes
                int innerLen = listPack.readInt(2);
                for (int j = 0; j < innerLen; j++) {
                    byte[] e = listPackEntry(listPack);
                    list.add(e);
                    if (list.size() == batch) {
                        emit(this.out, RPUSH, key, list);
                        list.clear();
                    }
                }
                int lpend = listPack.read(); // lp-end
                if (lpend != 255) {
                    throw new AssertionError("listpack expect 255 but " + lpend);
                }
            } else {
                throw new UnsupportedOperationException(String.valueOf(container));
            }
        }
        if (!list.isEmpty()) emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
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
    
    private void emit(OutputStream out, Function function, boolean replace) {
        // FUNCTION LOAD <ENGINE NAME> <LIBRARY NAME> [REPLACE] [DESC <LIBRARY DESCRIPTION>] <LIBRARY CODE>
        int count = 5;
        if (function.getDescription() != null) {
            count += 2;
        }
        if (replace) {
            count += 1;
        }
        OutputStreams.write(STAR, out);
        OutputStreams.write(String.valueOf(count).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        emitArg(FUNCTION_BUF);
        emitArg(LOAD_BUF);
        emitArg(ByteBuffer.wrap(function.getEngineName()));
        emitArg(ByteBuffer.wrap(function.getName()));
        if (function.getDescription() != null) {
            emitArg(DESCRIPTION_BUF);
            emitArg(ByteBuffer.wrap(function.getDescription()));
        }
        if (replace) {
            emitArg(REPLACE_BUF);
        }
        emitArg(ByteBuffer.wrap(function.getCode()));
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