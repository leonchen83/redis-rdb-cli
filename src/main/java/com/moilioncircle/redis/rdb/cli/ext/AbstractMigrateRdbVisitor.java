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

package com.moilioncircle.redis.rdb.cli.ext;

import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.RDB_OPCODE_FUNCTION;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.escape.RawEscaper;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpFunction;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractMigrateRdbVisitor extends AbstractRdbVisitor {

    protected final boolean flush;
    protected final boolean replace;
    protected MonitorManager manager;
    protected Escaper raw = new RawEscaper();

    public AbstractMigrateRdbVisitor(Replicator replicator, Configure configure, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) {
        super(replicator, configure, db, regexs, types);
        this.replace = replace;
        this.flush = configure.isMigrateFlush();
        this.manager = new MonitorManager(configure);
        this.manager.open("endpoint_statistics");
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) RDB_OPCODE_FUNCTION);
                SkipRdbParser parser = new SkipRdbParser(in);
                parser.rdbGenericLoadStringObject(); // name
                parser.rdbGenericLoadStringObject(); // engine name
                long hasDesc = parser.rdbLoadLen().len;
                if (hasDesc == 1) {
                    parser.rdbGenericLoadStringObject(); // description
                }
                parser.rdbGenericLoadStringObject(); // code
            }
            DumpFunction function = new DumpFunction();
            function.setSerialized(o.toByteArray());
            return function;
        }
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyString(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyList(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplySet(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyZSet(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        if (ver < 8 /* since redis rdb version 8 */) {
            // downgrade to RDB_TYPE_ZSET
            BaseRdbParser parser = new BaseRdbParser(in);
            BaseRdbEncoder encoder = new BaseRdbEncoder();
    
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                long len = parser.rdbLoadLen().len;
                long temp = len;
                while (len > 0) {
                    ByteArray element = parser.rdbLoadEncodedStringObject();
                    encoder.rdbGenericSaveStringObject(element, o);
                    double score = parser.rdbLoadBinaryDoubleValue();
                    encoder.rdbSaveDoubleValue(score, o);
                    len--;
                }
                ByteArrayOutputStream o1 = new ByteArrayOutputStream(configure.getOutputBufferSize());
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o1, raw, false)) {
                    listener.write((byte) RDB_TYPE_ZSET);
                    listener.handle(encoder.rdbSaveLen(temp));
                    listener.handle(o.toByteArray());
                }
        
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(RDB_TYPE_ZSET);
                dump.setKey(key);
                dump.setValue(o1.toByteArray());
                return context.valueOf(dump);
            }
        } else {
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                    listener.write((byte) type);
                    super.doApplyZSet2(in, version, key, contains, type, context);
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(type);
                dump.setKey(key);
                dump.setValue(o.toByteArray());
                return context.valueOf(dump);
            }
        }
        
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyHash(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyHashZipMap(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyListZipList(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplySetIntSet(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyZSetZipList(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        if (ver < 10 /* since redis rdb version 10 */) {
            // downgrade to RDB_TYPE_ZSET
            BaseRdbParser parser = new BaseRdbParser(in);
            BaseRdbEncoder encoder = new BaseRdbEncoder();
            
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
                listPack.skip(4); // total-bytes
                int len = listPack.readInt(2);
                long targetLen = len / 2;
                while (len > 0) {
                    byte[] element = listPackEntry(listPack);
                    encoder.rdbGenericSaveStringObject(new ByteArray(element), o);
                    len--;
                    double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
                    encoder.rdbSaveDoubleValue(score, o);
                    len--;
                }
                ByteArrayOutputStream o1 = new ByteArrayOutputStream(configure.getOutputBufferSize());
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o1, raw, false)) {
                    listener.write((byte) RDB_TYPE_ZSET);
                    listener.handle(encoder.rdbSaveLen(targetLen));
                    listener.handle(o.toByteArray());
                }
        
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(RDB_TYPE_ZSET);
                dump.setKey(key);
                dump.setValue(o1.toByteArray());
                return context.valueOf(dump);
            }
        } else {
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                    listener.write((byte) type);
                    super.doApplyZSetListPack(in, version, key, contains, type, context);
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(type);
                dump.setKey(key);
                dump.setValue(o.toByteArray());
                return context.valueOf(dump);
            }
        }
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyHashZipList(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        if (ver < 10 /* since redis rdb version 10 */) {
            // downgrade to RDB_TYPE_HASH
            BaseRdbParser parser = new BaseRdbParser(in);
            BaseRdbEncoder encoder = new BaseRdbEncoder();
    
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
                listPack.skip(4); // total-bytes
                int len = listPack.readInt(2);
                long targetLen = len / 2;
                while (len > 0) {
                    byte[] field = listPackEntry(listPack);
                    encoder.rdbGenericSaveStringObject(new ByteArray(field), o);
                    len--;
                    byte[] value = listPackEntry(listPack);
                    encoder.rdbGenericSaveStringObject(new ByteArray(value), o);
                    len--;
                }
                ByteArrayOutputStream o1 = new ByteArrayOutputStream(configure.getOutputBufferSize());
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o1, raw, false)) {
                    listener.write((byte) RDB_TYPE_HASH);
                    listener.handle(encoder.rdbSaveLen(targetLen));
                    listener.handle(o.toByteArray());
                }
        
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(RDB_TYPE_HASH);
                dump.setKey(key);
                dump.setValue(o1.toByteArray());
                return context.valueOf(dump);
            }
        } else {
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                    listener.write((byte) type);
                    super.doApplyHashListPack(in, version, key, contains, type, context);
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(type);
                dump.setKey(key);
                dump.setValue(o.toByteArray());
                return context.valueOf(dump);
            }
        }
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        if (ver < 7 /* since redis rdb version 7 */) {
            // downgrade to RDB_TYPE_LIST
            BaseRdbParser parser = new BaseRdbParser(in);
            BaseRdbEncoder encoder = new BaseRdbEncoder();
            
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                int total = 0;
                long len = parser.rdbLoadLen().len;
                for (long i = 0; i < len; i++) {
                    RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
            
                    BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                    BaseRdbParser.LenHelper.zltail(stream); // zltail
                    int zllen = BaseRdbParser.LenHelper.zllen(stream);
                    for (int j = 0; j < zllen; j++) {
                        byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                        encoder.rdbGenericSaveStringObject(new ByteArray(e), o);
                        total++;
                    }
                    int zlend = BaseRdbParser.LenHelper.zlend(stream);
                    if (zlend != 255) {
                        throw new AssertionError("zlend expect 255 but " + zlend);
                    }
                }
                ByteArrayOutputStream o1 = new ByteArrayOutputStream(configure.getOutputBufferSize());
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o1, raw, false)) {
                    listener.write((byte) RDB_TYPE_LIST);
                    listener.handle(encoder.rdbSaveLen(total));
                    listener.handle(o.toByteArray());
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(RDB_TYPE_LIST);
                dump.setKey(key);
                dump.setValue(o1.toByteArray());
                return context.valueOf(dump);
            }
        } else {
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                    listener.write((byte) type);
                    super.doApplyListQuickList(in, version, key, contains, type, context);
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(type);
                dump.setKey(key);
                dump.setValue(o.toByteArray());
                return context.valueOf(dump);
            }
        }
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        if (ver < 10 /* since redis rdb version 10 */) {
            // downgrade to RDB_TYPE_LIST
            BaseRdbParser parser = new BaseRdbParser(in);
            BaseRdbEncoder encoder = new BaseRdbEncoder();
            
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                int total = 0;
                long len = parser.rdbLoadLen().len;
                for (long i = 0; i < len; i++) {
                    long container = parser.rdbLoadLen().len;
                    ByteArray bytes = parser.rdbLoadPlainStringObject();
                    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                        encoder.rdbGenericSaveStringObject(new ByteArray(bytes.first()), o);
                        total++;
                    } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                        RedisInputStream listPack = new RedisInputStream(bytes);
                        listPack.skip(4); // total-bytes
                        int innerLen = listPack.readInt(2);
                        for (int j = 0; j < innerLen; j++) {
                            byte[] e = listPackEntry(listPack);
                            encoder.rdbGenericSaveStringObject(new ByteArray(e), o);
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
                ByteArrayOutputStream o1 = new ByteArrayOutputStream(configure.getOutputBufferSize());
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o1, raw, false)) {
                    listener.write((byte) RDB_TYPE_LIST);
                    listener.handle(encoder.rdbSaveLen(total));
                    listener.handle(o.toByteArray());
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(RDB_TYPE_LIST);
                dump.setKey(key);
                dump.setValue(o1.toByteArray());
                return context.valueOf(dump);
            }
        } else {
            try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
                try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                    listener.write((byte) type);
                    super.doApplyListQuickList2(in, version, key, contains, type, context);
                }
                DumpKeyValuePair dump = new DumpKeyValuePair();
                dump.setValueRdbType(type);
                dump.setKey(key);
                dump.setValue(o.toByteArray());
                return context.valueOf(dump);
            }
        }
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyModule(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyModule2(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                listener.write((byte) type);
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getOutputBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, ver, o, raw)) {
                if (ver < 10) {
                    listener.write((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
                } else {
                    listener.write((byte) type);
                }
                super.doApplyStreamListPacks2(in, ver, key, contains, type, context, listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            if (ver < 10) {
                dump.setValueRdbType((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
            } else {
                dump.setValueRdbType(type);
            }
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
}
