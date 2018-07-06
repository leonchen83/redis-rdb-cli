package com.moilioncircle.redis.cli.tool.ext.rct;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.DumpRawByteListener;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
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
public class DumpRdbVisitor extends AbstractRdbVisitor {

    private final int version;

    public DumpRdbVisitor(Replicator replicator,
                          Configure configure,
                          File out,
                          List<Long> db,
                          List<String> regexs,
                          List<Type> types,
                          Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
        this.version = -1;
    }

    @Override
    public Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadEncodedStringObject();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            while (len > 0) {
                skipParser.rdbLoadEncodedStringObject();
                len--;
            }
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
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
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
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
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
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
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            new SkipRdbParser(in).rdbLoadPlainStringObject();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipParser = new SkipRdbParser(in);
            long len = skipParser.rdbLoadLen().len;
            for (int i = 0; i < len; i++) {
                skipParser.rdbGenericLoadStringObject();
            }
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
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
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }

    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
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
        out.write('\n');
        return new DummyKeyValuePair();
    }
}
