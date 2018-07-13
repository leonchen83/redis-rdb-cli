package com.moilioncircle.redis.cli.tool.ext.rdt;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Guard;
import com.moilioncircle.redis.cli.tool.io.FilesOutputStream;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Baoyi Chen
 */
public class SplitRdbVisitor extends AbstractRdbVisitor {

    public SplitRdbVisitor(Replicator replicator, Configure configure, List<Long> db, List<String> regexs, List<DataType> types, Supplier<OutputStream> supplier) {
        super(replicator, configure, db, regexs, types, supplier);
        this.replicator.addEventListener((rep, event) -> {
            if (event instanceof PreRdbSyncEvent) {
                listener.reset(supplier.get());
            }
            if (event instanceof PostRdbSyncEvent) {
                FilesOutputStream out = listener.getOutputStream();
                out.writeCRC();
                OutputStreams.closeQuietly(out);
            }
        });
    }

    private void shard(byte[] key) {
        FilesOutputStream out = listener.getOutputStream();
        out.shard(key);
    }

    @Override
    public int applyVersion(RedisInputStream in) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyVersion(in);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public Event applyAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyAux(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public Event applyModuleAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyModuleAux(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applySelectDB(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyResizeDB(in, version, context);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }

    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyString(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyList(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplySet(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyZSet(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyZSet2(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyHash(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyHashZipMap(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyListZipList(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplySetIntSet(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyZSetZipList(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyHashZipList(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyListQuickList(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyModule(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyModule2(in, version, key, contains, type, context);
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        shard(key);
        return super.doApplyStreamListPacks(in, version, key, contains, type, context);
    }

}
