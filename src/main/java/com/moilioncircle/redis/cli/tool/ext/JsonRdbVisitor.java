package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;

/**
 * @author Baoyi Chen
 */
public class JsonRdbVisitor extends AbstractRdbVisitor {
    public JsonRdbVisitor(Replicator replicator,
                          File out,
                          List<Long> db,
                          List<String> regexs,
                          Long top,
                          List<Type> types,
                          Escape escape) throws Exception {
        super(replicator, out, db, regexs, top, types, escape);
    }

    private void emit(byte[] field, double value) throws IOException {
        emitString(field);
        out.write(':');
        escape.encode(value, out);
    }

    private void emit(byte[] field, byte[] value) throws IOException {
        emitString(field);
        out.write(':');
        emitString(value);
    }

    private void emitString(byte[] str) throws IOException {
        out.write('"');
        escape.encode(str, out);
        out.write('"');
    }

    @Override
    public String applyMagic(RedisInputStream in) throws IOException {
        out.write('[');
        return super.applyMagic(in);
    }

    private boolean first = true;
    private boolean hasdb = false;
    private boolean firstkey = true;

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        if (!first) {
            out.write('}');
            out.write(',');
            out.write('\n');
        }
        out.write('{');
        first = false;
        hasdb = true;
        DB db = super.applySelectDB(in, version);
        return db;
    }

    @Override
    public long applyEof(RedisInputStream in, int version) throws IOException {
        if (hasdb) out.write('}');
        out.write(']');
        return super.applyEof(in, version);
    }

    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        out.write('{');
        emit(key, val);
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        return kv;
    }

    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
            emit(element, score);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
            emit(element, score);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
            emit(field, value);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
            emit(field, value);
            flag = false;
            len--;
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
            emit(element, score);
            flag = false;
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        out.write(']');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
            emit(field, value);
            flag = false;
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        out.write('}');
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        if (!firstkey) {
            out.write(',');
            out.write('\n');
            firstkey = true;
        }
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
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        if (moduleParser == null) {
            throw new NoSuchElementException("module parser[" + moduleName + ", " + moduleVersion + "] not register. rdb type: [RDB_TYPE_MODULE]");
        }
        moduleParser.parse(in, 1);
        return new DummyKeyValuePair();
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadLen();
        skip.rdbLoadCheckModuleValue();
        return new DummyKeyValuePair();
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        SkipRdbParser skip = new SkipRdbParser(in);
        long listPacks = skip.rdbLoadLen().len;
        while (listPacks-- > 0) {
            skip.rdbLoadPlainStringObject();
            skip.rdbLoadPlainStringObject();
        }
        skip.rdbLoadLen();
        skip.rdbLoadLen();
        skip.rdbLoadLen();
        long groupCount = skip.rdbLoadLen().len;
        while (groupCount-- > 0) {
            skip.rdbLoadPlainStringObject();
            skip.rdbLoadLen();
            skip.rdbLoadLen();
            long groupPel = skip.rdbLoadLen().len;
            while (groupPel-- > 0) {
                in.skip(16);
                skip.rdbLoadMillisecondTime();
                skip.rdbLoadLen();
            }
            long consumerCount = skip.rdbLoadLen().len;
            while (consumerCount-- > 0) {
                skip.rdbLoadPlainStringObject();
                skip.rdbLoadMillisecondTime();
                long consumerPel = skip.rdbLoadLen().len;
                while (consumerPel-- > 0) {
                    in.skip(16);
                }
            }
        }
        return new DummyKeyValuePair();
    }
}