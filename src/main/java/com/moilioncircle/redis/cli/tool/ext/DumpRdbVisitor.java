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
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;
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
    
    private final int version;
    
    public DumpRdbVisitor(Replicator replicator, File out, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, keyRegEx, top, types, escape);
        this.version = -1;
    }
    
    private class DefaultRawByteListener implements RawByteListener, Closeable {
        private final int version;
        private final boolean contains;
        private final CRCOutputStream sub;
        
        private DefaultRawByteListener(byte type, int version, boolean contains) throws IOException {
            this.sub = new CRCOutputStream(out);
            this.contains = contains;
            if (contains) sub.write(type);
            int ver = DumpRdbVisitor.this.version;
            this.version = ver == -1 ? version : ver;
        }
        
        @Override
        public void handle(byte... rawBytes) {
            if (!contains) return;
            try {
                this.sub.write(rawBytes);
            } catch (IOException e) {
            }
        }
        
        public void close() throws IOException {
            if (!contains) return;
            this.sub.write((byte) version);
            this.sub.write((byte) 0x00);
            this.sub.write(this.sub.getCRC64());
            this.sub.write('\n');
        }
    }
    
    @Override
    public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_STRING, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_STRING, version, contains)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadEncodedStringObject();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_LIST, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applySet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_SET, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_SET, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyZSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_ZSET, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                skipParser.rdbLoadDoubleValue();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyZSet2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET_2, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_ZSET_2, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                skipParser.rdbLoadBinaryDoubleValue();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyHash(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_HASH, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                skipParser.rdbLoadEncodedStringObject();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyHashZipMap(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPMAP, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_HASH_ZIPMAP, version, contains)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyListZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST_ZIPLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_LIST_ZIPLIST, version, contains)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applySetIntSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_SET_INTSET, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_SET_INTSET, version, contains)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyZSetZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET_ZIPLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_ZSET_ZIPLIST, version, contains)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyHashZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_HASH_ZIPLIST, version, contains)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyListQuickList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST_QUICKLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_LIST_QUICKLIST, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            for (int i = 0; i < len; i++) {
                skipParser.rdbGenericLoadStringObject();
            }
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyModule(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_MODULE, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_MODULE, version, contains)) {
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
        }
        return null;
    }
    
    @Override
    public Event applyModule2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_MODULE_2, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_MODULE_2, version, contains)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
            replicator.removeRawByteListener(listener);
        }
        return null;
    }
    
    @Override
    public Event applyStreamListPacks(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_STREAM_LISTPACKS, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        try (DefaultRawByteListener listener = new DefaultRawByteListener((byte) RDB_TYPE_STREAM_LISTPACKS, version, contains)) {
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
        }
        return null;
    }
}
