package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_MODULE_OPCODE_EOF;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPMAP;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET_INTSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STRING;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_ZIPLIST;

/**
 * @author Baoyi Chen
 */
public class DumpRdbVisitor extends BaseRdbVisitor {

    private final int size;
    private final int version;

    public DumpRdbVisitor(Replicator replicator, File out, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, keyRegEx, top, types, escape);
        this.version = -1;
        this.size = 8192;
    }

    private class DefaultRawByteListener implements RawByteListener {
        private final int version;
        private final CRCOutputStream sub;

        private DefaultRawByteListener(byte type, int version) throws IOException {
            this.sub = new CRCOutputStream(out);
            sub.write(type);
            int ver = DumpRdbVisitor.this.version;
            this.version = ver == -1 ? version : ver;
        }

        @Override
        public void handle(byte... rawBytes) {
            for (byte b : rawBytes) {
                try {
                    this.sub.write(b);
                } catch (IOException e) {
                }
            }
        }

        public void end() throws IOException {
            this.sub.write((byte) version);
            this.sub.write((byte) 0x00);
            this.sub.write(this.sub.getCRC64());
        }
    }

    @Override
    public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o0 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_STRING, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadEncodedStringObject();
        replicator.removeRawByteListener(listener);
        o0.setValueRdbType(RDB_TYPE_STRING);
        o0.setValue(listener.end());
        o0.setDb(db);
        o0.setKey(key);
        return o0;
    }

    @Override
    public Event applyList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o1 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_LIST, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        replicator.removeRawByteListener(listener);
        o1.setValueRdbType(RDB_TYPE_LIST);
        o1.setValue(listener.end());
        o1.setDb(db);
        o1.setKey(key);
        return o1;
    }

    @Override
    public Event applySet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o2 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_SET, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        replicator.removeRawByteListener(listener);
        o2.setValueRdbType(RDB_TYPE_SET);
        o2.setValue(listener.end());
        o2.setDb(db);
        o2.setKey(key);
        return o2;
    }

    @Override
    public Event applyZSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o3 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_ZSET, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadDoubleValue();
            len--;
        }
        replicator.removeRawByteListener(listener);
        o3.setValueRdbType(RDB_TYPE_ZSET);
        o3.setValue(listener.end());
        o3.setDb(db);
        o3.setKey(key);
        return o3;
    }

    @Override
    public Event applyZSet2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o5 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_ZSET_2, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadBinaryDoubleValue();
            len--;
        }
        replicator.removeRawByteListener(listener);
        o5.setValueRdbType(RDB_TYPE_ZSET_2);
        o5.setValue(listener.end());
        o5.setDb(db);
        o5.setKey(key);
        return o5;
    }

    @Override
    public Event applyHash(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o4 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_HASH, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        replicator.removeRawByteListener(listener);
        o4.setValueRdbType(RDB_TYPE_HASH);
        o4.setValue(listener.end());
        o4.setDb(db);
        o4.setKey(key);
        return o4;
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o9 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_HASH_ZIPMAP, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        o9.setValueRdbType(RDB_TYPE_HASH_ZIPMAP);
        o9.setValue(listener.end());
        o9.setDb(db);
        o9.setKey(key);
        return o9;
    }

    @Override
    public Event applyListZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o10 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_LIST_ZIPLIST, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        o10.setValueRdbType(RDB_TYPE_LIST_ZIPLIST);
        o10.setValue(listener.end());
        o10.setDb(db);
        o10.setKey(key);
        return o10;
    }

    @Override
    public Event applySetIntSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o11 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_SET_INTSET, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        o11.setValueRdbType(RDB_TYPE_SET_INTSET);
        o11.setValue(listener.end());
        o11.setDb(db);
        o11.setKey(key);
        return o11;
    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o12 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_ZSET_ZIPLIST, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        o12.setValueRdbType(RDB_TYPE_ZSET_ZIPLIST);
        o12.setValue(listener.end());
        o12.setDb(db);
        o12.setKey(key);
        return o12;
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o13 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_HASH_ZIPLIST, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        o13.setValueRdbType(RDB_TYPE_HASH_ZIPLIST);
        o13.setValue(listener.end());
        o13.setDb(db);
        o13.setKey(key);
        return o13;
    }

    @Override
    public Event applyListQuickList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o14 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_LIST_QUICKLIST, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            skipParser.rdbGenericLoadStringObject();
        }
        replicator.removeRawByteListener(listener);
        o14.setValueRdbType(RDB_TYPE_LIST_QUICKLIST);
        o14.setValue(listener.end());
        o14.setDb(db);
        o14.setKey(key);
        return o14;
    }

    @Override
    public Event applyModule(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o6 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_MODULE, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        char[] c = new char[9];
        long moduleid = skipParser.rdbLoadLen().len;
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
        replicator.removeRawByteListener(listener);
        o6.setValueRdbType(RDB_TYPE_MODULE);
        o6.setValue(listener.end());
        o6.setDb(db);
        o6.setKey(key);
        return o6;
    }

    @Override
    public Event applyModule2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o7 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_MODULE_2, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        char[] c = new char[9];
        long moduleid = skipParser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        if (moduleParser == null) {
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
        } else {
            moduleParser.parse(in, 2);
            long eof = parser.rdbLoadLen().len;
            if (eof != RDB_MODULE_OPCODE_EOF) {
                throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
            }
        }
        replicator.removeRawByteListener(listener);
        o7.setValueRdbType(RDB_TYPE_MODULE_2);
        o7.setValue(listener.end());
        o7.setDb(db);
        o7.setKey(key);
        return o7;
    }

    @Override
    public Event applyStreamListPacks(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o15 = new DumpKeyValuePair();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_STREAM_LISTPACKS, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long listPacks = skipParser.rdbLoadLen().len;
        while (listPacks-- > 0) {
            skipParser.rdbLoadPlainStringObject();
            skipParser.rdbLoadPlainStringObject();
        }
        skipParser.rdbLoadLen();
        skipParser.rdbLoadLen();
        skipParser.rdbLoadLen();
        long groupCount = skipParser.rdbLoadLen().len;
        while (groupCount-- > 0) {
            skipParser.rdbLoadPlainStringObject();
            skipParser.rdbLoadLen();
            skipParser.rdbLoadLen();
            long groupPel = skipParser.rdbLoadLen().len;
            while (groupPel-- > 0) {
                in.skip(16);
                skipParser.rdbLoadMillisecondTime();
                skipParser.rdbLoadLen();
            }
            long consumerCount = skipParser.rdbLoadLen().len;
            while (consumerCount-- > 0) {
                skipParser.rdbLoadPlainStringObject();
                skipParser.rdbLoadMillisecondTime();
                long consumerPel = skipParser.rdbLoadLen().len;
                while (consumerPel-- > 0) {
                    in.skip(16);
                }
            }
        }
        replicator.removeRawByteListener(listener);
        o15.setValueRdbType(RDB_TYPE_STREAM_LISTPACKS);
        o15.setValue(listener.end());
        o15.setDb(db);
        o15.setKey(key);
        return o15;
    }
}
