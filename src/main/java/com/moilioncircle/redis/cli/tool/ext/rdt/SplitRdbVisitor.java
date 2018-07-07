package com.moilioncircle.redis.cli.tool.ext.rdt;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Guard;
import com.moilioncircle.redis.cli.tool.io.FilesOutputStream;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostFullSyncEvent;
import com.moilioncircle.redis.replicator.event.PreFullSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Baoyi Chen
 */
public class SplitRdbVisitor extends AbstractRdbVisitor {
    
    public SplitRdbVisitor(Replicator replicator,
                           Configure configure,
                           List<Long> db,
                           List<String> regexs,
                           List<DataType> types,
                           Supplier<OutputStream> supplier) {
        super(replicator, configure, db, regexs, types, supplier);
        this.replicator.addEventListener((rep, event) -> {
            if (event instanceof PreFullSyncEvent) {
                listener.reset(supplier.get());
            }
            if (event instanceof PostFullSyncEvent) {
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
    public DB applyResizeDB(RedisInputStream in, DB db, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyResizeDB(in, db, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyString(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyList(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplySet(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyZSet(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyZSet2(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyHash(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyHashZipMap(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyListZipList(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplySetIntSet(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyZSetZipList(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyHashZipList(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyListQuickList(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyModule(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyModule2(in, db, version, key, contains, type);
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        shard(key);
        return super.doApplyStreamListPacks(in, db, version, key, contains, type);
    }
    
}
