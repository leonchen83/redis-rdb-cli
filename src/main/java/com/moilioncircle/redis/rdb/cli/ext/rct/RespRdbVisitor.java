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

import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.escape.RawEscaper;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
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
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        emit(this.out, SELECT, String.valueOf(db.getDbNumber()).getBytes());
        return db;
    }
    
    @Override
    public Event applyExpireTime(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTime(in, version, context);
        if (!kv.isContains() || kv.getKey() == null) return context.valueOf(kv);
        emit(this.out, EXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredSeconds() * 1000).getBytes());
        return context.valueOf(kv);
    }
    
    @Override
    public Event applyExpireTimeMs(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTimeMs(in, version, context);
        if (!kv.isContains() || kv.getKey() == null) return context.valueOf(kv);
        emit(this.out, EXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredMs()).getBytes());
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener ignored = new DumpRawByteListener(replicator, (byte) type, version, out, escaper)) {
                super.doApplyModule(in, version, key, contains, type, context);
            }
            if (replace) {
                emit(this.out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(this.out, RESTORE, key, ex, out.toByteArray());
            }
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule2(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener ignored = new DumpRawByteListener(replicator, (byte) type, version, out, escaper)) {
                super.doApplyModule2(in, version, key, contains, type, context);
            }
            if (replace) {
                emit(this.out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(this.out, RESTORE, key, ex, out.toByteArray());
            }
            return context.valueOf(new DummyKeyValuePair());
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyStreamListPacks(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener ignored = new DumpRawByteListener(replicator, (byte) type, version, out, escaper)) {
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
            }
            if (replace) {
                emit(this.out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(this.out, RESTORE, key, ex, out.toByteArray());
            }
            return context.valueOf(new DummyKeyValuePair());
        }
    }
}