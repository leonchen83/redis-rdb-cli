package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;

/**
 * @author Baoyi Chen
 */
public class KeyRdbVisitor extends BaseRdbVisitor {
    public KeyRdbVisitor(Replicator replicator, File out, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, keyRegEx, top, types, escape);
    }
    
    @Override
    public Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadEncodedStringObject();
        return null;
    }
    
    @Override
    public Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }
    
    @Override
    public Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        while (len > 0) {
            skip.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }
    
    @Override
    public Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
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
    public Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
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
    public Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
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
    public Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }
    
    @Override
    public Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }
    
    @Override
    public Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }
    
    @Override
    public Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }
    
    @Override
    public Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadPlainStringObject();
        return null;
    }
    
    @Override
    public Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        long len = skip.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            skip.rdbGenericLoadStringObject();
        }
        return null;
    }
    
    @Override
    public Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
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
    public Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
        SkipRdbParser skip = new SkipRdbParser(in);
        skip.rdbLoadLen();
        skip.rdbLoadCheckModuleValue();
        return null;
    }
    
    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape(escape, key, out);
        out.write('\n');
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