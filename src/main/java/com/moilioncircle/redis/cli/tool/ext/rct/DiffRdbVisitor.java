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
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class DiffRdbVisitor extends AbstractRdbVisitor {

    public DiffRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types) {
        super(replicator, configure, out, db, regexs, types, Escape.REDIS);
    }

    @Override
    public Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyString(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyList(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplySet(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyZSet(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyZSet2(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyHash(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyHashZipMap(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyListZipList(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplySetIntSet(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyZSetZipList(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyHashZipList(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyListQuickList(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyModule2(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }

    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        escape.encode(key, out, configure);
        delimiter(out);
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
            replicator.addRawByteListener(listener);
            super.doApplyStreamListPacks(in, version, key, contains, type, context);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return context.valueOf(new DummyKeyValuePair());
    }
}
