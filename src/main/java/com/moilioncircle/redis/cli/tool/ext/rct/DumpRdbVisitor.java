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
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class DumpRdbVisitor extends AbstractRdbVisitor {
    
    private final int version;
    
    public DumpRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape) {
        super(replicator, configure, out, db, regexs, types, escape);
        this.version = -1;
    }
    
    @Override
    public Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        escape.encode(key, out);
        out.write(' ');
        try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape)) {
            replicator.addRawByteListener(listener);
            super.doApplyString(in, db, version, key, contains, type);
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
            super.doApplyList(in, db, version, key, contains, type);
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
            super.doApplySet(in, db, version, key, contains, type);
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
            super.doApplyZSet(in, db, version, key, contains, type);
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
            super.doApplyZSet2(in, db, version, key, contains, type);
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
            super.doApplyHash(in, db, version, key, contains, type);
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
            super.doApplyHashZipMap(in, db, version, key, contains, type);
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
            super.doApplyListZipList(in, db, version, key, contains, type);
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
            super.doApplySetIntSet(in, db, version, key, contains, type);
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
            super.doApplyZSetZipList(in, db, version, key, contains, type);
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
            super.doApplyHashZipList(in, db, version, key, contains, type);
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
            super.doApplyListQuickList(in, db, version, key, contains, type);
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
            super.doApplyModule(in, db, version, key, contains, type);
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
            super.doApplyModule2(in, db, version, key, contains, type);
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
            super.doApplyStreamListPacks(in, db, version, key, contains, type);
            replicator.removeRawByteListener(listener);
        }
        out.write('\n');
        return new DummyKeyValuePair();
    }
}
