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
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
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
public class ValRdbVisitor extends BaseRdbVisitor {
    public ValRdbVisitor(Replicator replicator, File out, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, keyRegEx, top, types, escape);
    }

    @Override
    public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |       <content>       |
         * |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        if (contains(db.getDbNumber(), RDB_TYPE_STRING, Strings.toString(key))) {
            escape(escape, key, out);
            out.write(' ');
            escape(escape, val, out);
            out.write('\n');
            out.flush();
        }
        return null;
    }

    @Override
    public Event applyList(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            if (contains) {
                escape(escape, element, out);
                if (len - 1 > 0) out.write(',');
                else out.write('\n');
            }
            len--;
        }
        return null;
    }

    @Override
    public Event applySet(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_SET, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            if (contains) {
                escape(escape, element, out);
                if (len - 1 > 0) out.write(',');
                else out.write('\n');
            }
            len--;
        }
        return null;
    }

    @Override
    public Event applyZSet(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |        <score>       |
         * | 1 or 5 bytes |    string contents    |    double content    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            if (contains) {
                escape(escape, element, out);
                out.write(' ');
                escape(escape, score, out);
                if (len - 1 > 0) out.write(',');
                else out.write('\n');
            }
            len--;
        }
        return null;
    }

    @Override
    public Event applyZSet2(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |        <score>       |
         * | 1 or 5 bytes |    string contents    |    binary double     |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET_2, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        /* rdb version 8*/
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            if (contains) {
                escape(escape, element, out);
                out.write(' ');
                escape(escape, score, out);
                if (len - 1 > 0) out.write(',');
                else out.write('\n');
            }
            len--;
        }
        return null;
    }

    @Override
    public Event applyHash(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            if (contains) {
                escape(escape, field, out);
                out.write(' ');
                escape(escape, value, out);
                if (len - 1 > 0) out.write(',');
                else out.write('\n');
            }
            len--;
        }
        return null;
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |<zmlen> |   <len>     |"foo"    |    <len>   | <free> |   "bar" |<zmend> |
         * | 1 byte | 1 or 5 byte | content |1 or 5 byte | 1 byte | content | 1 byte |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPMAP, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                if (contains) {
                    out.write('\n');
                }
                return null;
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                //value is null
                if (contains) {
                    escape(escape, field, out);
                    out.write(' ');
                    escape(escape, null, out);
                    out.write(',');
                }
                return null;
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            if (contains) {
                escape(escape, field, out);
                out.write(' ');
                escape(escape, value, out);
                out.write(',');
            }
        }
    }

    @Override
    public Event applyListZipList(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_LIST_ZIPLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        for (int i = 0; i < zllen; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            if (contains) {
                escape(escape, e, out);
                if (i + 1 < zllen) out.write(',');
                else out.write('\n');
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return null;
    }

    @Override
    public Event applySetIntSet(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |<encoding>| <length-of-contents>|              <contents>                            |
         * | 4 bytes  |            4 bytes  | 2 bytes element| 4 bytes element | 8 bytes element |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_SET_INTSET, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
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
            if (contains) {
                escape(escape, element.getBytes(), out);
                if (i + 1 < lenOfContent) out.write(',');
                else out.write('\n');
            }
        }
        return null;
    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_ZSET_ZIPLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            if (contains) {
                escape(escape, element, out);
                out.write(' ');
                escape(escape, score, out);
                if (zllen - 1 > 0) out.write(',');
                else out.write('\n');
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return null;
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, DB db, int version) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_HASH_ZIPLIST, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write(' ');
        }
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            if (contains) {
                escape(escape, field, out);
                out.write(' ');
                escape(escape, value, out);
                if (zllen - 1 > 0) out.write(',');
                else out.write('\n');
            }
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
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
        long len = parser.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));

            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                if (contains) {
                    escape(escape, e, out);
                    if (i + 1 < len) out.write(',');
                    else if (j + 1 < zllen) out.write(',');
                    else out.write('\n');
                }
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        return null;
    }

    @Override
    public Event applyModule(RedisInputStream in, DB db, int version) throws IOException {
        //|6|6|6|6|6|6|6|6|6|10|
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_MODULE, Strings.toString(key));
        if (contains) {
            escape(escape, key, out);
            out.write('\n');
        }
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
        return null;
    }

    @Override
    public Event applyModule2(RedisInputStream in, DB db, int version) throws IOException {
        //|6|6|6|6|6|6|6|6|6|10|
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] rawKey = parser.rdbLoadEncodedStringObject().first();
        boolean contains = contains(db.getDbNumber(), RDB_TYPE_MODULE_2, Strings.toString(rawKey));
        if (contains) {
            escape(escape, rawKey, out);
            out.write('\n');
        }
        parser.rdbLoadLen();
        SkipRdbParser skipRdbParser = new SkipRdbParser(in);
        skipRdbParser.rdbLoadCheckModuleValue();
        return null;
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
            escape(escape, key, out);
            out.write('\n');
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