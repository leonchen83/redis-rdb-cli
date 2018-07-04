package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.cli.tool.util.Closes;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

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
import static java.util.stream.Collectors.toList;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractRdbVisitor extends DefaultRdbVisitor {

    protected Set<Long> db;
    protected Escape escape;
    protected List<Type> types;
    protected OutputStream out;
    protected Set<String> keys;
    protected List<Pattern> regexs;

    public AbstractRdbVisitor(Replicator replicator,
                              File output,
                              List<Long> db,
                              List<String> regexs,
                              List<Type> types,
                              Escape escape) throws Exception {
        this(replicator, db, regexs, types);
        this.escape = escape;
        this.out = new BufferedOutputStream(new FileOutputStream(output));
        replicator.addCloseListener(r -> Closes.closeQuietly(out));
    }

    public AbstractRdbVisitor(Replicator replicator,
                              List<Long> db,
                              List<String> regexs,
                              List<Type> types) {
        super(replicator);
        this.types = types;
        this.db = new HashSet<>(db);
        this.keys = new HashSet<>(regexs);
        this.regexs = regexs.stream().map(Pattern::compile).collect(toList());
    }

    protected boolean contains(int type) {
        return Type.contains(types, type);
    }

    protected boolean contains(long db) {
        return this.db.isEmpty() || this.db.contains(db);
    }

    protected boolean contains(String key) {
        if (keys.isEmpty() || keys.contains(key)) return true;
        for (Pattern pattern : regexs) {
            if (pattern.matcher(Strings.toString(key)).matches()) return true;
        }
        return false;
    }

    protected boolean contains(long db, int type, String key) {
        return contains(db) && contains(type) && contains(key);
    }

    @Override
    public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_STRING, Strings.toString(key));
        if (contains) {
            Event event = doApplyString(in, db, version, key, contains, RDB_TYPE_STRING);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadEncodedStringObject();
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST, Strings.toString(key));
        if (contains) {
            Event event = doApplyList(in, db, version, key, contains, RDB_TYPE_LIST);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            long len = skip.rdbLoadLen().len;
            while (len > 0) {
                skip.rdbLoadEncodedStringObject();
                len--;
            }
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applySet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_SET, Strings.toString(key));
        if (contains) {
            Event event = doApplySet(in, db, version, key, contains, RDB_TYPE_SET);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadEncodedStringObject();
            long len = skip.rdbLoadLen().len;
            while (len > 0) {
                skip.rdbLoadEncodedStringObject();
                len--;
            }
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyZSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET, Strings.toString(key));
        if (contains) {
            Event event = doApplyZSet(in, db, version, key, contains, RDB_TYPE_ZSET);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            long len = skip.rdbLoadLen().len;
            while (len > 0) {
                skip.rdbLoadEncodedStringObject();
                skip.rdbLoadDoubleValue();
                len--;
            }
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyZSet2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET_2, Strings.toString(key));
        if (contains) {
            Event event = doApplyZSet2(in, db, version, key, contains, RDB_TYPE_ZSET_2);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            long len = skip.rdbLoadLen().len;
            while (len > 0) {
                skip.rdbLoadEncodedStringObject();
                skip.rdbLoadBinaryDoubleValue();
                len--;
            }
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyHash(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH, Strings.toString(key));
        if (contains) {
            Event event = doApplyHash(in, db, version, key, contains, RDB_TYPE_HASH);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            long len = skip.rdbLoadLen().len;
            while (len > 0) {
                skip.rdbLoadEncodedStringObject();
                skip.rdbLoadEncodedStringObject();
                len--;
            }
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPMAP, Strings.toString(key));
        if (contains) {
            Event event = doApplyHashZipMap(in, db, version, key, contains, RDB_TYPE_HASH_ZIPMAP);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadPlainStringObject();
            return new DummyKeyValuePair();
        }

    }

    @Override
    public Event applyListZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST_ZIPLIST, Strings.toString(key));
        if (contains) {
            Event event = doApplyListZipList(in, db, version, key, contains, RDB_TYPE_LIST_ZIPLIST);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadPlainStringObject();
            return new DummyKeyValuePair();
        }

    }

    @Override
    public Event applySetIntSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_SET_INTSET, Strings.toString(key));
        if (contains) {
            Event event = doApplySetIntSet(in, db, version, key, contains, RDB_TYPE_SET_INTSET);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadPlainStringObject();
            return new DummyKeyValuePair();
        }

    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET_ZIPLIST, Strings.toString(key));
        if (contains) {
            Event event = doApplyZSetZipList(in, db, version, key, contains, RDB_TYPE_ZSET_ZIPLIST);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadPlainStringObject();
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPLIST, Strings.toString(key));
        if (contains) {
            Event event = doApplyHashZipList(in, db, version, key, contains, RDB_TYPE_HASH_ZIPLIST);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadPlainStringObject();
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyListQuickList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST_QUICKLIST, Strings.toString(key));
        if (contains) {
            Event event = doApplyListQuickList(in, db, version, key, contains, RDB_TYPE_LIST_QUICKLIST);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            long len = skip.rdbLoadLen().len;
            for (int i = 0; i < len; i++) {
                skip.rdbGenericLoadStringObject();
            }
            return new DummyKeyValuePair();
        }
    }

    @Override
    public Event applyModule(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_MODULE, Strings.toString(key));
        if (contains) {
            Event event = doApplyModule(in, db, version, key, contains, RDB_TYPE_MODULE);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            char[] c = new char[9];
            long moduleid = skip.rdbLoadLen().len;
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
    }

    @Override
    public Event applyModule2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_MODULE_2, Strings.toString(key));
        if (contains) {
            Event event = doApplyModule2(in, db, version, key, contains, RDB_TYPE_MODULE_2);
            return event;
        } else {
            SkipRdbParser skip = new SkipRdbParser(in);
            skip.rdbLoadLen();
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
            return new DummyKeyValuePair();
        }
    }

    protected ModuleParser<? extends Module> lookupModuleParser(String moduleName, int moduleVersion) {
        return replicator.getModuleParser(moduleName, moduleVersion);
    }

    @Override
    @SuppressWarnings("resource")
    public Event applyStreamListPacks(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_STREAM_LISTPACKS, Strings.toString(key));
        if (contains) {
            Event event = doApplyStreamListPacks(in, db, version, key, contains, RDB_TYPE_STREAM_LISTPACKS);
            return event;
        } else {
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

    protected abstract Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;

    protected abstract Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException;
}
