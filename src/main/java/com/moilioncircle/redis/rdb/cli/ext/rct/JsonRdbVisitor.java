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

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.DumpRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.Escape;
import com.moilioncircle.redis.rdb.cli.glossary.JsonEscape;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
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
public class JsonRdbVisitor extends AbstractRdbVisitor {
    
    private boolean first = true;
    private boolean hasdb = false;
    private JsonEscape jsonEscape;
    private boolean firstkey = true;
    
    public JsonRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
        this.jsonEscape = new JsonEscape(escape);
    }

    private void emitZSet(byte[] field, double value) {
        emitString(field);
        OutputStreams.write(':', out);
        jsonEscape.encode(value, out, configure);
    }

    private void emitField(byte[] field, byte[] value) {
        emitString(field);
        OutputStreams.write(':', out);
        emitString(value);
    }

    private void emitString(byte[] str) {
        OutputStreams.write('"', out);
        jsonEscape.encode(str, out, configure);
        OutputStreams.write('"', out);
    }
    
    @Override
    public String applyMagic(RedisInputStream in) throws IOException {
        OutputStreams.write('[', out);
        return super.applyMagic(in);
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        if (!first) {
            OutputStreams.write('}', out);
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        first = false;
        firstkey = true;
        hasdb = true;
        OutputStreams.write('{', out);
        return super.applySelectDB(in, version);
    }
    
    @Override
    public long applyEof(RedisInputStream in, int version) throws IOException {
        if (hasdb) OutputStreams.write('}', out);
        OutputStreams.write(']', out);
        return super.applyEof(in, version);
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        emitField(key, val);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
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
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
        OutputStreams.write('"', out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, jsonEscape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        OutputStreams.write('"', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
        OutputStreams.write('"', out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, jsonEscape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule2(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        OutputStreams.write('"', out);
        return context.valueOf(new DummyKeyValuePair());
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            OutputStreams.write(',', out);
            OutputStreams.write('\n', out);
        }
        firstkey = false;
        emitString(key);
        OutputStreams.write(':', out);
        OutputStreams.write('"', out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, jsonEscape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyStreamListPacks(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        OutputStreams.write('"', out);
        return context.valueOf(new DummyKeyValuePair());
    }
}