package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
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
public class KeyRdbVisitor extends BaseRdbVisitor {
    public KeyRdbVisitor(Replicator replicator, File out, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, keyRegEx, top, types, escape);
    }

    @Override
    public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_STRING, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadEncodedStringObject();
        return null;
    }

    @Override
    public Event applyList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_LIST, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }

    @Override
    public Event applySet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_SET, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }

    @Override
    public Event applyZSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_ZSET, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            skip.rdbLoadDoubleValue();
            len--;
        }
        return null;
    }

    @Override
    public Event applyZSet2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_ZSET_2, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            skip.rdbLoadBinaryDoubleValue();
            len--;
        }
        return null;
    }

    @Override
    public Event applyHash(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_HASH, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            skip.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPMAP, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyListZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_LIST_ZIPLIST, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applySetIntSet(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_SET_INTSET, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_ZSET_ZIPLIST, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPLIST, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyListQuickList(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_LIST_QUICKLIST, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            skip.rdbGenericLoadStringObject();
        }
        return null;
    }

    @Override
    public Event applyModule(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_MODULE, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
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
        return null;
    }

    @Override
    public Event applyModule2(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_MODULE_2, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
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
            skip.rdbLoadCheckModuleValue();
        } else {
            moduleParser.parse(in, 2);
            long eof = parser.rdbLoadLen().len;
            if (eof != RDB_MODULE_OPCODE_EOF) {
                throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
            }
        }
        return null;
    }

    @Override
    public Event applyStreamListPacks(RedisInputStream in, DB db, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_STREAM_LISTPACKS, Strings.toString(key))) {
            escape(escape, key, out);
            out.write('\n');
            out.flush();
        }
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
        return null;
    }
}