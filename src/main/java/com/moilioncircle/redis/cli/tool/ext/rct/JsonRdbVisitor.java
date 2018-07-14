package com.moilioncircle.redis.cli.tool.ext.rct;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.DumpRawByteListener;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;

/**
 * @author Baoyi Chen
 */
public class JsonRdbVisitor extends AbstractRdbVisitor {

    private boolean first = true;
    private boolean hasdb = false;
    private boolean firstkey = true;

    public JsonRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
    }
    
    private void emitZSet(byte[] field, double value) throws IOException {
        emitString(field);
        out.write(':');
        escape.encode(value, out, configure);
    }
    
    private void emitField(byte[] field, byte[] value) throws IOException {
        emitString(field);
        out.write(':');
        emitString(value);
    }

    private void emitString(byte[] str) throws IOException {
        out.write('"');
        escape.encode(str, out, configure);
        out.write('"');
    }

    @Override
    public String applyMagic(RedisInputStream in) throws IOException {
        out.write('[');
        return super.applyMagic(in);
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        if (!first) {
            out.write('}');
            out.write(',');
            out.write('\n');
        }
        first = false;
        firstkey = true;
        hasdb = true;
        out.write('{');
        return super.applySelectDB(in, version);
    }

    @Override
    public long applyEof(RedisInputStream in, int version) throws IOException {
        if (hasdb) out.write('}');
        out.write(']');
        return super.applyEof(in, version);
    }

    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        emit(key, val);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('[');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        boolean flag = true;
        while (len > 0) {
            if (!flag) {
                out.write(',');
            }
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            emitString(element);
            flag = false;
            len--;
        }
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('[');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        boolean flag = true;
        while (len > 0) {
            if (!flag) {
                out.write(',');
            }
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            emitString(element);
            flag = false;
            len--;
        }
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('{');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        boolean flag = true;
        while (len > 0) {
            if (!flag) {
                out.write(',');
            }
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            emitZSet(element, score);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('{');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        boolean flag = true;
        while (len > 0) {
            if (!flag) {
                out.write(',');
            }
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            emitZSet(element, score);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('{');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        boolean flag = true;
        while (len > 0) {
            if (!flag) {
                out.write(',');
            }
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            emitField(field, value);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('{');
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        boolean flag = true;
        while (len > 0) {
            if (!flag) {
                out.write(',');
            }
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            emitField(field, value);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('[');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        boolean flag = true;
        for (int i = 0; i < zllen; i++) {
            if (!flag) {
                out.write(',');
            }
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            emitString(e);
            flag = false;
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('[');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        boolean flag = true;
        int encoding = BaseRdbParser.LenHelper.encoding(stream);
        long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
        for (long i = 0; i < lenOfContent; i++) {
            if (!flag) {
                out.write(',');
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
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('[');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        boolean flag = true;
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            if (!flag) {
                out.write(',');
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
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('{');
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        boolean flag = true;
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            if (!flag) {
                out.write(',');
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
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('[');
        BaseRdbParser parser = new BaseRdbParser(in);
        boolean flag = true;
        long len = parser.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));

            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                if (!flag) {
                    out.write(',');
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
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('"');
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('"');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('"');
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule2(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('"');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
        }
        firstkey = false;
        emitString(key);
        out.write(':');
        out.write('"');
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyStreamListPacks(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('"');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return context.valueOf(kv);
    }
}