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

import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.DEL;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.EXPIREAT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.FIELDS;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.HMSET;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.HPEXPIREAT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ONE;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.PEXPIREAT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.RPUSH;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.SADD;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.SELECT;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.SET;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ZADD;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ZERO;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ZERO_BUF;
import static com.moilioncircle.redis.rdb.cli.util.Collections.isEmpty;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static java.nio.ByteBuffer.wrap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.io.LayeredOutputStream;
import com.moilioncircle.redis.rdb.cli.net.protocol.Protocols;
import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Function;
import com.moilioncircle.redis.replicator.rdb.datatype.TTLValue;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;
import com.moilioncircle.redis.replicator.util.TTLByteArrayMap;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public class RespRdbVisitor extends AbstractRctRdbVisitor {
    
    private final int batch;
    private final boolean replace;
    
    public RespRdbVisitor(Replicator replicator, Configure configure, Args.RctArgs args, Escaper escaper) {
        super(replicator, configure, args, escaper);
        this.batch = configure.getBatchSize();
        this.replace = args.replace;
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        Function function = (Function) super.applyFunction(in, version);
        Protocols.functionLoad(this.out, function, replace);
        return function;
    }
    
    @Override
    public Event applyFunction2(RedisInputStream in, int version) throws IOException {
        Function function = (Function) super.applyFunction2(in, version);
        Protocols.functionLoad(this.out, function, replace);
        return function;
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        Protocols.emit(this.out, SELECT, String.valueOf(db.getDbNumber()).getBytes());
        return db;
    }
    
    @Override
    public Event applyExpireTime(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTime(in, version, context);
        if (!kv.isContains() || kv.getKey() == null) return context.valueOf(kv);
        Protocols.emit(this.out, EXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredSeconds()).getBytes());
        return context.valueOf(kv);
    }
    
    @Override
    public Event applyExpireTimeMs(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTimeMs(in, version, context);
        if (!kv.isContains() || kv.getKey() == null) return context.valueOf(kv);
        Protocols.emit(this.out, PEXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredMs()).getBytes());
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        Protocols.emit(this.out, SET, key, val);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            if (list.size() == batch) {
                Protocols.emit(this.out, RPUSH, key, list);
                list.clear();
            }
            len--;
        }
        if (!isEmpty(list)) Protocols.emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            if (list.size() == batch) {
                Protocols.emit(this.out, SADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!isEmpty(list)) Protocols.emit(this.out, SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplySetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = listPackEntry(listPack);
            len--;
            list.add(element);
            if (list.size() == batch) {
                Protocols.emit(this.out, SADD, key, list);
                list.clear();
            }
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        if (!isEmpty(list)) Protocols.emit(this.out, SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                Protocols.emit(this.out, ZADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!isEmpty(list)) Protocols.emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                Protocols.emit(this.out, ZADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!isEmpty(list)) Protocols.emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                Protocols.emit(this.out, HMSET, key, list);
                list.clear();
            }
            len--;
        }
        if (!isEmpty(list)) Protocols.emit(this.out, HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        List<byte[]> list = new ArrayList<>();
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                if (!isEmpty(list)) {
                    Protocols.emit(this.out, HMSET, key, list);
                    list.clear();
                }
                DummyKeyValuePair kv = new DummyKeyValuePair();
                kv.setValueRdbType(type);
                kv.setKey(key);
                kv.setContains(true);
                return context.valueOf(kv);
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                list.add(field);
                list.add(null);
                Protocols.emit(this.out, HMSET, key, list);
                list.clear();
                DummyKeyValuePair kv = new DummyKeyValuePair();
                kv.setValueRdbType(type);
                kv.setKey(key);
                kv.setContains(true);
                return context.valueOf(kv);
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                Protocols.emit(this.out, HMSET, key, list);
                list = new ArrayList<>();
            }
        }
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                Protocols.emit(this.out, RPUSH, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!isEmpty(list)) Protocols.emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                Protocols.emit(this.out, SADD, key, list);
                list.clear();
            }
        }
        if (!isEmpty(list)) Protocols.emit(this.out, SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                Protocols.emit(this.out, ZADD, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!isEmpty(list)) Protocols.emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                Protocols.emit(this.out, ZADD, key, list);
                list.clear();
            }
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        if (!isEmpty(list)) Protocols.emit(this.out, ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                Protocols.emit(this.out, HMSET, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!isEmpty(list)) Protocols.emit(this.out, HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                Protocols.emit(this.out, HMSET, key, list);
                list.clear();
            }
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        if (!isEmpty(list)) Protocols.emit(this.out, HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
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
                    Protocols.emit(this.out, RPUSH, key, list);
                    list.clear();
                }
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        if (!isEmpty(list)) Protocols.emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> list = new ArrayList<>();
        long len = parser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            long container = parser.rdbLoadLen().len;
            ByteArray bytes = parser.rdbLoadPlainStringObject();
            if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                list.add(bytes.first());
                if (list.size() == batch) {
                    Protocols.emit(this.out, RPUSH, key, list);
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
                        Protocols.emit(this.out, RPUSH, key, list);
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
        if (!isEmpty(list)) Protocols.emit(this.out, RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashMetadata(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long minExpire = parser.rdbLoadMillisecondTime();
        long len = parser.rdbLoadLen().len;
        Map<byte[], Tuple2<byte[], byte[]>> map = new LinkedHashMap<>((int) len);
        while (len > 0) {
            long ttl = parser.rdbLoadLen().len;
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            ttl = ttl != 0L ? ttl + minExpire - 1L : 0L;
            map.put(field, Tuples.of(String.valueOf(ttl).getBytes(), value));
            if (map.size() == batch) {
                ttlHash(key, map);
                map.clear();
            }
            len--;
        }
        if (!map.isEmpty()) {
            ttlHash(key, map);
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    @Override
    protected Event doApplyHashListPackEx(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        if (replace) Protocols.emit(this.out, DEL, key);
        BaseRdbParser parser = new BaseRdbParser(in);
        long minExpire = parser.rdbLoadMillisecondTime();
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        Map<byte[], Tuple2<byte[], byte[]>> map = new LinkedHashMap<>(len / 3);
        while (len > 0) {
            byte[] field = listPackEntry(listPack);
            len--;
            byte[] value = listPackEntry(listPack);
            len--;
            byte[] ttl = listPackEntry(listPack);
            len--;
            map.put(field, Tuples.of(ttl, value));
            if (map.size() == batch) {
                ttlHash(key, map);
                map.clear();
            }
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        if (!map.isEmpty()) {
            ttlHash(key, map);
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(true);
        return context.valueOf(kv);
    }
    
    private void ttlHash(byte[] key, Map<byte[], Tuple2<byte[], byte[]>> map) {
        List<byte[]> list = new ArrayList<>(map.size() * 2);
        for (Map.Entry<byte[], Tuple2<byte[], byte[]>> entry : map.entrySet()) {
            list.add(entry.getKey());
            list.add(entry.getValue().getV2());
        }
        Protocols.emit(this.out, HMSET, key, list);
        for (Map.Entry<byte[], Tuple2<byte[], byte[]>> entry : map.entrySet()) {
            byte[] v = entry.getValue().getV1();
            if (!Arrays.equals(v, ZERO)) {
                Protocols.emit(this.out, HPEXPIREAT, key, v, FIELDS, ONE, entry.getKey());
            }
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
    
    @Override
    protected Event doApplyStreamListPacks3(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        ByteBuffer ex = ZERO_BUF;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyStreamListPacks3(in, version, key, type, context);
            } else {
                ex = wrap(String.valueOf(ms).getBytes());
            }
        }
        version = getVersion(version);
        try (LayeredOutputStream out = new LayeredOutputStream(configure)) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, escaper)) {
                if (version < 11) {
                    listener.write((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
                } else {
                    listener.write((byte) type);
                }
                super.doApplyStreamListPacks3(in, version, key, type, context, listener);
            }
            Protocols.restore(this.out, wrap(key), ex, out.toByteBuffers(), replace);
            return context.valueOf(new DummyKeyValuePair());
        }
    }
}