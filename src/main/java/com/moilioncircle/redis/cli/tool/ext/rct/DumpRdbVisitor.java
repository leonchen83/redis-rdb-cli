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
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class DumpRdbVisitor extends AbstractRdbVisitor {
    
    private final boolean replace;
    
    private static final byte[] ZERO = "0".getBytes();
    private static final byte[] SELECT = "select".getBytes();
    private static final byte[] REPLACE = "replace".getBytes();
    private static final byte[] RESTORE = "restore".getBytes();
    
    public DumpRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) {
        super(replicator, configure, out, db, regexs, types, Escape.RAW);
        this.replace = replace;
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        long dbnum = db.getDbNumber();
        emit(out, SELECT, String.valueOf(dbnum).getBytes());
        return db;
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyString(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyString(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyList(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplySet(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplySet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSet(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSet2(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSet2(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHash(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHash(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashZipMap(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHashZipMap(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListZipList(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyListZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplySetIntSet(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplySetIntSet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyZSetZipList(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSetZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyHashZipList(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHashZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyListQuickList(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyListQuickList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyModule2(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule2(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        byte[] ex = ZERO;
        if (context.getExpiredValue() != null) {
            long ms = context.getExpiredValue() - System.currentTimeMillis();
            if (ms <= 0) {
                return super.doApplyStreamListPacks(in, version, key, contains, type, context);
            } else {
                ex = String.valueOf(ms).getBytes();
            }
        }
        version = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, version, out, escape, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            if (replace) {
                emit(out, RESTORE, key, ex, out.toByteArray(), REPLACE);
            } else {
                emit(out, RESTORE, key, ex, out.toByteArray());
            }
            DummyKeyValuePair kv = new DummyKeyValuePair();
            kv.setValueRdbType(type);
            kv.setKey(key);
            kv.setContains(contains);
            return context.valueOf(kv);
        }
    }
}
