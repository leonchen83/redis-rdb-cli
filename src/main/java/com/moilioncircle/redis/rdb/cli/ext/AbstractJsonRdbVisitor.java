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
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;

import java.io.File;
import java.io.IOException;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.escape.RedisEscaper;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractJsonRdbVisitor extends AbstractRdbVisitor {

    private boolean firstkey = true;
    private Escaper redis;
    
    public AbstractJsonRdbVisitor(Replicator replicator, Configure configure, File out, Filter filter, Escaper escaper) {
        super(replicator, configure, out, filter, escaper);
        this.redis = new RedisEscaper(configure.getDelimiter(), configure.getQuote());
    }

    private void emitString(byte[] str) {
        OutputStreams.write('"', out);
        escaper.encode(str, out);
        OutputStreams.write('"', out);
    }

    private void emitField(String field, byte[] value) {
        emitField(field.getBytes(), value);
    }

    private void emitField(String field, String value) {
        emitField(field.getBytes(), value.getBytes());
    }

    private void emitField(byte[] field, byte[] value) {
        emitString(field);
        OutputStreams.write(':', out);
        emitString(value);
    }

    private void emitNull(byte[] field) {
        emitString(field);
        OutputStreams.write(':', out);
        escaper.encode("null".getBytes(), out);
    }

    private void emitZSet(byte[] field, double value) {
        emitString(field);
        OutputStreams.write(':', out);
        escaper.encode(value, out);
    }

    private void emitField(String field, long value) {
        emitString(field.getBytes());
        OutputStreams.write(':', out);
        escaper.encode(String.valueOf(value).getBytes(), out);
    }
    
    /**
     * 
     */
    protected abstract void separator();
    
    public static interface Emitable {
        void emitValue() throws IOException;
    }

    protected void json(ContextKeyValuePair context, byte[] key, int type, Emitable emitable) throws IOException {
        if (!firstkey) {
            separator();
        }
        firstkey = false;
        OutputStreams.write('{', out);
        emitField("key", key);
        OutputStreams.write(',', out);
        emitString("value".getBytes());
        OutputStreams.write(':', out);
        emitable.emitValue();
        if (configure.isExportMeta()) {
            OutputStreams.write(',', out);
            emitField("db", context.getDb().getDbNumber());
            OutputStreams.write(',', out);
            emitField("type", DataType.parse(type).getValue());
            ExpiredType expiry = context.getExpiredType();
            if (expiry != ExpiredType.NONE) {
                OutputStreams.write(',', out);
                if (expiry == ExpiredType.SECOND) {
                    emitField("expiry", context.getExpiredValue() * 1000);
                } else {
                    emitField("expiry", context.getExpiredValue());
                }
            }
        }
        OutputStreams.write('}', out);
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] val = parser.rdbLoadEncodedStringObject().first();
            emitString(val);
        });
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('[', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] element = parser.rdbLoadEncodedStringObject().first();
                emitString(element);
                flag = false;
                len--;
            }
            OutputStreams.write(']', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('[', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] element = parser.rdbLoadEncodedStringObject().first();
                emitString(element);
                flag = false;
                len--;
            }
            OutputStreams.write(']', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] element = parser.rdbLoadEncodedStringObject().first();
                double score = parser.rdbLoadDoubleValue();
                emitZSet(element, score);
                flag = false;
                len--;
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] element = parser.rdbLoadEncodedStringObject().first();
                double score = parser.rdbLoadBinaryDoubleValue();
                emitZSet(element, score);
                flag = false;
                len--;
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            long len = parser.rdbLoadLen().len;
            boolean flag = true;
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] field = parser.rdbLoadEncodedStringObject().first();
                byte[] value = parser.rdbLoadEncodedStringObject().first();
                emitField(field, value);
                flag = false;
                len--;
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
            BaseRdbParser.LenHelper.zmlen(stream); // zmlen
            boolean flag = true;
            while (true) {
                int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
                if (zmEleLen == 255) {
                    break;
                }
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                flag = false;
                byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
                zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
                if (zmEleLen == 255) {
                    //value is null
                    emitNull(field);
                    break;
                }
                int free = BaseRdbParser.LenHelper.free(stream);
                byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
                BaseRdbParser.StringHelper.skip(stream, free);
                emitField(field, value);
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('[', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            boolean flag = true;
            for (int i = 0; i < zllen; i++) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                emitString(e);
                flag = false;
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
            OutputStreams.write(']', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('[', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
            boolean flag = true;
            int encoding = BaseRdbParser.LenHelper.encoding(stream);
            long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
            for (long i = 0; i < lenOfContent; i++) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
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
                emitString(element.getBytes());
                flag = false;
            }
            OutputStreams.write(']', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
            boolean flag = true;
            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            while (zllen > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
                zllen--;
                double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
                zllen--;
                emitZSet(element, score);
                flag = false;
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
            boolean flag = true;
            listPack.skip(4); // total-bytes
            int len = listPack.readInt(2);
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] element = listPackEntry(listPack);
                len--;
                double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
                len--;
                emitZSet(element, score);
                flag = false;
            }
            int lpend = listPack.read(); // lp-end
            if (lpend != 255) {
                throw new AssertionError("listpack expect 255 but " + lpend);
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            boolean flag = true;
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            while (zllen > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
                zllen--;
                byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
                zllen--;
                emitField(field, value);
                flag = false;
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('{', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
            boolean flag = true;
            listPack.skip(4); // total-bytes
            int len = listPack.readInt(2);
            while (len > 0) {
                if (!flag) {
                    OutputStreams.write(',', out);
                }
                byte[] field = listPackEntry(listPack);
                len--;
                byte[] value = listPackEntry(listPack);
                len--;
                emitField(field, value);
                flag = false;
            }
            int lpend = listPack.read(); // lp-end
            if (lpend != 255) {
                throw new AssertionError("listpack expect 255 but " + lpend);
            }
            OutputStreams.write('}', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('[', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            boolean flag = true;
            long len = parser.rdbLoadLen().len;
            for (long i = 0; i < len; i++) {
                RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));

                BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                BaseRdbParser.LenHelper.zltail(stream); // zltail
                int zllen = BaseRdbParser.LenHelper.zllen(stream);
                for (int j = 0; j < zllen; j++) {
                    if (!flag) {
                        OutputStreams.write(',', out);
                    }
                    byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                    emitString(e);
                    flag = false;
                }
                int zlend = BaseRdbParser.LenHelper.zlend(stream);
                if (zlend != 255) {
                    throw new AssertionError("zlend expect 255 but " + zlend);
                }
            }
            OutputStreams.write(']', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('[', out);
            BaseRdbParser parser = new BaseRdbParser(in);
            boolean flag = true;
            long len = parser.rdbLoadLen().len;
            for (long i = 0; i < len; i++) {
                long container = parser.rdbLoadLen().len;
                ByteArray bytes = parser.rdbLoadPlainStringObject();
                if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                    if (!flag) {
                        OutputStreams.write(',', out);
                    }
                    emitString(bytes.first());
                    flag = false;
                } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                    RedisInputStream listPack = new RedisInputStream(bytes);
                    listPack.skip(4); // total-bytes
                    int innerLen = listPack.readInt(2);
                    for (int j = 0; j < innerLen; j++) {
                        if (!flag) {
                            OutputStreams.write(',', out);
                        }
                        byte[] e = listPackEntry(listPack);
                        emitString(e);
                        flag = false;
                    }
                    int lpend = listPack.read(); // lp-end
                    if (lpend != 255) {
                        throw new AssertionError("listpack expect 255 but " + lpend);
                    }
                } else {
                    throw new UnsupportedOperationException(String.valueOf(container));
                }
            }
            OutputStreams.write(']', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('"', out);
            int v = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, v, out, redis)) {
                listener.write((byte) type);
                super.doApplyModule(in, version, key, contains, type, context);
            }
            OutputStreams.write('"', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('"', out);
            int v = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, v, out, redis)) {
                listener.write((byte) type);
                super.doApplyModule2(in, version, key, contains, type, context);
            }
            OutputStreams.write('"', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('"', out);
            int v = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, v, out, redis)) {
                listener.write((byte) type);
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
            }
            OutputStreams.write('"', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        json(context, key, type, () -> {
            OutputStreams.write('"', out);
            int v = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
            try (DumpRawByteListener listener = new DumpRawByteListener(replicator, v, out, redis)) {
                if (v < 10) {
                    listener.write((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
                } else {
                    listener.write((byte) type);
                }
                super.doApplyStreamListPacks2(in, v, key, contains, type, context, listener);
            }
            OutputStreams.write('"', out);
        });
        return context.valueOf(new DummyKeyValuePair());
    }
}