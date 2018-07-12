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
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.STAR;

/**
 * @author Baoyi Chen
 */
public class RespRdbVisitor extends AbstractRdbVisitor {

    private final int batch;

    private static final byte[] ZERO = "0".getBytes();
    private static final byte[] SET = "set".getBytes();
    private static final byte[] SADD = "sadd".getBytes();
    private static final byte[] ZADD = "zadd".getBytes();
    private static final byte[] RPUSH = "rpush".getBytes();
    private static final byte[] HMSET = "hmset".getBytes();
    private static final byte[] SELECT = "select".getBytes();
    private static final byte[] RESTORE = "restore".getBytes();
    private static final byte[] EXPIREAT = "expireat".getBytes();

    public RespRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types) {
        super(replicator, configure, out, db, regexs, types, Escape.REDIS);
        this.batch = configure.getBatchSize();
    }

    private void emit(byte[] command, byte[] key) throws IOException {
        out.write(STAR);
        out.write(String.valueOf(3).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] c = escape.encode(command, configure);
        out.write(String.valueOf(c.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(c);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] k = escape.encode(key, configure);
        out.write(String.valueOf(k.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(k);
        out.write('\r');
        out.write('\n');
    }

    private void emit(byte[] command, byte[] key, byte[] ary) throws IOException {
        out.write(STAR);
        out.write(String.valueOf(3).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] c = escape.encode(command, configure);
        out.write(String.valueOf(c.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(c);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] k = escape.encode(key, configure);
        out.write(String.valueOf(k.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(k);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] a = escape.encode(ary, configure);
        out.write(String.valueOf(a.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(a);
        out.write('\r');
        out.write('\n');
    }

    private void emit(byte[] command, byte[] key, byte[] ary1, byte[] ary2) throws IOException {
        out.write(STAR);
        out.write(String.valueOf(3).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] c = escape.encode(command, configure);
        out.write(String.valueOf(c.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(c);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] k = escape.encode(key, configure);
        out.write(String.valueOf(k.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(k);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] a = escape.encode(ary1, configure);
        out.write(String.valueOf(a.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(a);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        out.write(String.valueOf(ary2.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(ary2);
        out.write('\r');
        out.write('\n');
    }

    private void emit(byte[] command, byte[] key, List<byte[]> ary) throws IOException {
        out.write(STAR);
        out.write(String.valueOf(ary.size() + 2).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] c = escape.encode(command, configure);
        out.write(String.valueOf(c.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(c);
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        byte[] k = escape.encode(key, configure);
        out.write(String.valueOf(k.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(k);
        out.write('\r');
        out.write('\n');
        for (final byte[] arg : ary) {
            out.write(DOLLAR);
            byte[] a = escape.encode(arg, configure);
            out.write(String.valueOf(a.length).getBytes());
            out.write('\r');
            out.write('\n');
            out.write(a);
            out.write('\r');
            out.write('\n');
        }
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        long dbnum = db.getDbNumber();
        emit(SELECT, String.valueOf(dbnum).getBytes());
        return db;
    }

    @Override
    public Event applyExpireTime(RedisInputStream in, DB db, int version) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTime(in, db, version);
        if (!kv.isContains() || kv.getKey() == null) return kv;
        emit(EXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredSeconds() * 1000).getBytes());
        return kv;
    }

    @Override
    public Event applyExpireTimeMs(RedisInputStream in, DB db, int version) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTimeMs(in, db, version);
        if (!kv.isContains() || kv.getKey() == null) return kv;
        emit(EXPIREAT, kv.getKey(), String.valueOf(kv.getExpiredMs()).getBytes());
        return kv;
    }

    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        emit(SET, key, val);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            if (list.size() == batch) {
                emit(RPUSH, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            if (list.size() == batch) {
                emit(SADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                emit(ZADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            list.add(String.valueOf(score).getBytes());
            list.add(element);
            if (list.size() == 2 * batch) {
                emit(ZADD, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ArrayList<>();
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                emit(HMSET, key, list);
                list.clear();
            }
            len--;
        }
        if (!list.isEmpty()) emit(HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        List<byte[]> list = new ArrayList<>();
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                if (!list.isEmpty()) {
                    emit(HMSET, key, list);
                    list.clear();
                }
                DummyKeyValuePair kv = new DummyKeyValuePair();
                kv.setDb(db);
                kv.setValueRdbType(type);
                kv.setKey(key);
                kv.setContains(contains);
                return kv;
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                list.add(field);
                list.add(null);
                emit(HMSET, key, list);
                list.clear();
                DummyKeyValuePair kv = new DummyKeyValuePair();
                kv.setDb(db);
                kv.setValueRdbType(type);
                kv.setKey(key);
                kv.setContains(contains);
                return kv;
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            list.add(field);
            list.add(value);
            if (list.size() == 2 * batch) {
                emit(HMSET, key, list);
                list = new ArrayList<>();
            }
        }
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
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
                emit(RPUSH, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!list.isEmpty()) emit(RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
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
                emit(SADD, key, list);
                list.clear();
            }
        }
        if (!list.isEmpty()) emit(SADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
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
                emit(ZADD, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!list.isEmpty()) emit(ZADD, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
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
                emit(HMSET, key, list);
                list.clear();
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        if (!list.isEmpty()) emit(HMSET, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> list = new ArrayList<>();
        long len = parser.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));

            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                list.add(e);
                if (list.size() == batch) {
                    emit(RPUSH, key, list);
                    list.clear();
                }
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        if (!list.isEmpty()) emit(RPUSH, key, list);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
            }
            emit(RESTORE, key, ZERO, out.toByteArray());
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setDb(db);
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return kv;
        }
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule2(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
            }
            emit(RESTORE, key, ZERO, out.toByteArray());
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setDb(db);
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return kv;
        }
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyStreamListPacks(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
            }
            emit(RESTORE, key, ZERO, out.toByteArray());
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setDb(db);
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return kv;
        }
    }
}