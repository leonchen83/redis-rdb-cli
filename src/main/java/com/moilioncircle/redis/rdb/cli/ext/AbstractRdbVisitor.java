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

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.Escape;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.moilioncircle.redis.rdb.cli.glossary.Guard.DRAIN;
import static com.moilioncircle.redis.rdb.cli.glossary.Guard.PASS;
import static com.moilioncircle.redis.rdb.cli.glossary.Guard.SAVE;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
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
import static com.moilioncircle.redis.replicator.Constants.STAR;
import static java.util.stream.Collectors.toList;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractRdbVisitor extends DefaultRdbVisitor {

    protected static final byte[] ZERO = "0".getBytes();
    protected static final byte[] SET = "set".getBytes();
    protected static final byte[] SADD = "sadd".getBytes();
    protected static final byte[] ZADD = "zadd".getBytes();
    protected static final byte[] RPUSH = "rpush".getBytes();
    protected static final byte[] HMSET = "hmset".getBytes();
    protected static final byte[] SELECT = "select".getBytes();
    protected static final byte[] REPLACE = "replace".getBytes();
    protected static final byte[] RESTORE = "restore".getBytes();
    protected static final byte[] EXPIREAT = "expireat".getBytes();
    protected static final byte[] RESTORE_ASKING = "restore-asking".getBytes();

    // common
    protected Set<Long> db;
    protected Set<String> keys;
    protected Configure configure;
    protected List<DataType> types;
    protected List<Pattern> regexs;
    //rct
    protected Escape escape;
    protected OutputStream out;
    //rdt
    protected GuardRawByteListener listener;

    /**
     * rmt
     */
    public AbstractRdbVisitor(Replicator replicator, Configure configure, List<Long> db, List<String> regexs, List<DataType> types) {
        super(replicator);
        this.types = types;
        this.configure = configure;
        this.db = new HashSet<>(db);
        this.keys = new HashSet<>(regexs);
        this.regexs = regexs.stream().map(Pattern::compile).collect(toList());
    }

    /**
     * rct
     */
    public AbstractRdbVisitor(Replicator replicator, Configure configure, File output, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        this(replicator, configure, db, regexs, types);
        this.escape = escape;
        replicator.addEventListener((rep, event) -> {
            if (event instanceof PreRdbSyncEvent) {
                OutputStreams.closeQuietly(this.out);
                this.out = OutputStreams.newBufferedOutputStream(output, configure.getBufferSize());
            }
        });
        replicator.addCloseListener(rep -> OutputStreams.closeQuietly(out));
    }

    /**
     * rdt
     */
    public AbstractRdbVisitor(Replicator replicator, Configure configure, List<Long> db, List<String> regexs, List<DataType> types, Supplier<OutputStream> supplier) {
        this(replicator, configure, db, regexs, types);
        this.listener = new GuardRawByteListener(configure.getBufferSize(), supplier.get());
        this.replicator.addRawByteListener(listener);
    }

    protected boolean contains(int type) {
        return DataType.contains(types, type);
    }

    protected boolean contains(long db) {
        return this.db.isEmpty() || this.db.contains(db);
    }

    protected boolean contains(String key) {
        if (keys.isEmpty() || keys.contains(key)) return true;
        for (Pattern pattern : regexs) {
            if (pattern.matcher(key).matches()) return true;
        }
        return false;
    }

    protected boolean contains(long db, int type, String key) {
        return contains(db) && contains(type) && contains(key);
    }

    protected void delimiter(OutputStream out) {
        OutputStreams.write(configure.getDelimiter(), out);
    }

    protected void quote(byte[] bytes, OutputStream out) {
        quote(bytes, out, true);
    }

    protected void quote(byte[] bytes, OutputStream out, boolean escape) {
        OutputStreams.write(configure.getQuote(), out);
        if (escape) {
            this.escape.encode(bytes, out, configure);
        } else {
            OutputStreams.write(bytes, out);
        }
        OutputStreams.write(configure.getQuote(), out);
    }

    protected void emit(OutputStream out, byte[] command, byte[]... ary) {
        OutputStreams.write(STAR, out);
        OutputStreams.write(String.valueOf(ary.length + 1).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(DOLLAR, out);
        OutputStreams.write(String.valueOf(command.length).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(command, out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        for (final byte[] arg : ary) {
            OutputStreams.write(DOLLAR, out);
            OutputStreams.write(String.valueOf(arg.length).getBytes(), out);
            OutputStreams.write('\r', out);
            OutputStreams.write('\n', out);
            OutputStreams.write(arg, out);
            OutputStreams.write('\r', out);
            OutputStreams.write('\n', out);
        }
    }

    protected void emit(OutputStream out, byte[] command, byte[] key, List<byte[]> ary) {
        OutputStreams.write(STAR, out);
        OutputStreams.write(String.valueOf(ary.size() + 2).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(DOLLAR, out);
        OutputStreams.write(String.valueOf(command.length).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(command, out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(DOLLAR, out);
        OutputStreams.write(String.valueOf(key.length).getBytes(), out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        OutputStreams.write(key, out);
        OutputStreams.write('\r', out);
        OutputStreams.write('\n', out);
        for (final byte[] arg : ary) {
            OutputStreams.write(DOLLAR, out);
            OutputStreams.write(String.valueOf(arg.length).getBytes(), out);
            OutputStreams.write('\r', out);
            OutputStreams.write('\n', out);
            OutputStreams.write(arg, out);
            OutputStreams.write('\r', out);
            OutputStreams.write('\n', out);
        }
    }

    @Override
    public Event applyString(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_STRING, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyString(in, version, key, contains, RDB_TYPE_STRING, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadEncodedStringObject();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_LIST, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyList(in, version, key, contains, RDB_TYPE_LIST, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                long len = skip.rdbLoadLen().len;
                while (len > 0) {
                    skip.rdbLoadEncodedStringObject();
                    len--;
                }
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applySet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_SET, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplySet(in, version, key, contains, RDB_TYPE_SET, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                long len = skip.rdbLoadLen().len;
                while (len > 0) {
                    skip.rdbLoadEncodedStringObject();
                    len--;
                }
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyZSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyZSet(in, version, key, contains, RDB_TYPE_ZSET, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                long len = skip.rdbLoadLen().len;
                while (len > 0) {
                    skip.rdbLoadEncodedStringObject();
                    skip.rdbLoadDoubleValue();
                    len--;
                }
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyZSet2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET_2, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyZSet2(in, version, key, contains, RDB_TYPE_ZSET_2, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                long len = skip.rdbLoadLen().len;
                while (len > 0) {
                    skip.rdbLoadEncodedStringObject();
                    skip.rdbLoadBinaryDoubleValue();
                    len--;
                }
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyHash(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_HASH, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyHash(in, version, key, contains, RDB_TYPE_HASH, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                long len = skip.rdbLoadLen().len;
                while (len > 0) {
                    skip.rdbLoadEncodedStringObject();
                    skip.rdbLoadEncodedStringObject();
                    len--;
                }
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_HASH_ZIPMAP, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyHashZipMap(in, version, key, contains, RDB_TYPE_HASH_ZIPMAP, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadPlainStringObject();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyListZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_LIST_ZIPLIST, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyListZipList(in, version, key, contains, RDB_TYPE_LIST_ZIPLIST, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadPlainStringObject();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applySetIntSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_SET_INTSET, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplySetIntSet(in, version, key, contains, RDB_TYPE_SET_INTSET, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadPlainStringObject();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET_ZIPLIST, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyZSetZipList(in, version, key, contains, RDB_TYPE_ZSET_ZIPLIST, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadPlainStringObject();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_HASH_ZIPLIST, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyHashZipList(in, version, key, contains, RDB_TYPE_HASH_ZIPLIST, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadPlainStringObject();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyListQuickList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_LIST_QUICKLIST, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyListQuickList(in, version, key, contains, RDB_TYPE_LIST_QUICKLIST, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                long len = skip.rdbLoadLen().len;
                for (long i = 0; i < len; i++) {
                    skip.rdbGenericLoadStringObject();
                }
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyModule(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_MODULE, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyModule(in, version, key, contains, RDB_TYPE_MODULE, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
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
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyModule2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_MODULE_2, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyModule2(in, version, key, contains, RDB_TYPE_MODULE_2, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
                SkipRdbParser skip = new SkipRdbParser(in);
                skip.rdbLoadLen();
                SkipRdbParser skipRdbParser = new SkipRdbParser(in);
                skipRdbParser.rdbLoadCheckModuleValue();
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    @Override
    public Event applyStreamListPacks(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        try {
            BaseRdbParser parser = new BaseRdbParser(in);
            byte[] key = parser.rdbLoadEncodedStringObject().first();
            boolean contains = contains(context.getDb().getDbNumber(), RDB_TYPE_STREAM_LISTPACKS, Strings.toString(key));
            if (contains) {
                if (listener != null) listener.setGuard(DRAIN);
                return doApplyStreamListPacks(in, version, key, contains, RDB_TYPE_STREAM_LISTPACKS, context);
            } else {
                if (listener != null) listener.setGuard(PASS);
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
                return context.valueOf(new DummyKeyValuePair());
            }
        } finally {
            if (listener != null) listener.setGuard(SAVE);
        }
    }

    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        new SkipRdbParser(in).rdbLoadEncodedStringObject();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadDoubleValue();
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadBinaryDoubleValue();
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            skipParser.rdbGenericLoadStringObject();
        }
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        SkipRdbParser skipRdbParser = new SkipRdbParser(in);
        skipRdbParser.rdbLoadCheckModuleValue();
        return context.valueOf(new DummyKeyValuePair());
    }

    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
        return context.valueOf(new DummyKeyValuePair());
    }
}
