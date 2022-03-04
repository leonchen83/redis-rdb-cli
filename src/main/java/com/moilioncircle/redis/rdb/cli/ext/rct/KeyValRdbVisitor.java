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

import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;

import java.io.File;
import java.io.IOException;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.escape.RedisEscaper;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * @author Baoyi Chen
 */
public class KeyValRdbVisitor extends AbstractRctRdbVisitor {
    
    private Escaper redis;
    
    public KeyValRdbVisitor(Replicator replicator, Configure configure, Filter filter, File output, Escaper escaper) {
        super(replicator, configure, filter, output, escaper);
        this.redis = new RedisEscaper(configure.getDelimiter(), configure.getQuote());
    }
    
    @Override
    public Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        quote(val, out);
        Outputs.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            quote(element, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            quote(element, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            quote(element, out);
            delimiter(out);
            escaper.encode(score, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            quote(element, out);
            delimiter(out);
            escaper.encode(score, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            quote(field, out);
            delimiter(out);
            quote(value, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        boolean flag = true;
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                Outputs.write('\n', out);
                return context.valueOf(new DummyKeyValuePair());
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                if (!flag) delimiter(out);
                quote(field, out);
                delimiter(out);
                quote(null, out);
                Outputs.write('\n', out);
                return context.valueOf(new DummyKeyValuePair());
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            if (flag) flag = false;
            else delimiter(out);
            quote(field, out);
            delimiter(out);
            quote(value, out);
        }
    }
    
    @Override
    public Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        for (int i = 0; i < zllen; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            quote(e, out);
            if (i + 1 < zllen) delimiter(out);
            else Outputs.write('\n', out);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
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
            quote(element.getBytes(), out);
            if (i + 1 < lenOfContent) delimiter(out);
            else Outputs.write('\n', out);
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            quote(element, out);
            delimiter(out);
            escaper.encode(score, out);
            if (zllen - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] element = listPackEntry(listPack);
            len--;
            double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
            len--;
            quote(element, out);
            delimiter(out);
            escaper.encode(score, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            quote(field, out);
            delimiter(out);
            quote(value, out);
            if (zllen - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] field = listPackEntry(listPack);
            len--;
            byte[] value = listPackEntry(listPack);
            len--;
            quote(field, out);
            delimiter(out);
            quote(value, out);
            if (len - 1 > 0) delimiter(out);
            else Outputs.write('\n', out);
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));
            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                quote(e, out);
                if (i + 1 < len) delimiter(out);
                else if (j + 1 < zllen) delimiter(out);
                else Outputs.write('\n', out);
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            long container = parser.rdbLoadLen().len;
            ByteArray bytes = parser.rdbLoadPlainStringObject();
            if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                quote(bytes.first(), out);
                if (i + 1 < len) delimiter(out);
                else Outputs.write('\n', out);
            } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                RedisInputStream listPack = new RedisInputStream(bytes);
                listPack.skip(4); // total-bytes
                int innerLen = listPack.readInt(2);
                for (int j = 0; j < innerLen; j++) {
                    byte[] e = listPackEntry(listPack);
                    quote(e, out);
                    if (i + 1 < len) delimiter(out);
                    else if (j + 1 < innerLen) delimiter(out);
                    else Outputs.write('\n', out);
                }
                int lpend = listPack.read(); // lp-end
                if (lpend != 255) {
                    throw new AssertionError("listpack expect 255 but " + lpend);
                }
            } else {
                throw new UnsupportedOperationException(String.valueOf(container));
            }
        }
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        out.write(configure.getQuote());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, redis)) {
            listener.write((byte) type);
            super.doApplyModule(in, version, key, type, context);
        }
        Outputs.write('\n', out);
        out.write(configure.getQuote());
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        out.write(configure.getQuote());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, redis)) {
            listener.write((byte) type);
            super.doApplyModule2(in, version, key, type, context);
        }
        out.write(configure.getQuote());
        Outputs.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    protected ModuleParser<? extends Module> lookupModuleParser(String moduleName, int moduleVersion) {
        return replicator.getModuleParser(moduleName, moduleVersion);
    }
    
    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        out.write(configure.getQuote());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, redis)) {
            listener.write((byte) type);
            super.doApplyStreamListPacks(in, version, key, type, context);
        }
        out.write(configure.getQuote());
        Outputs.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    public Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        delimiter(out);
        out.write(configure.getQuote());
        version = getVersion(version);
        try (DumpRawByteListener listener = new DumpRawByteListener(replicator, version, out, redis)) {
            if (version < 10) {
                listener.write((byte) Constants.RDB_TYPE_STREAM_LISTPACKS);
            } else {
                listener.write((byte) type);
            }
            super.doApplyStreamListPacks2(in, version, key, type, context, listener);
        }
        out.write(configure.getQuote());
        Outputs.write('\n', out);
        return context.valueOf(new DummyKeyValuePair());
    }
}