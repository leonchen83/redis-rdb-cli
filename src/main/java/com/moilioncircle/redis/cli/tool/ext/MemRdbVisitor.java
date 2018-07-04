package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.cli.tool.util.MinHeap;
import com.moilioncircle.redis.cli.tool.util.type.Tuple2;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostFullSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static com.moilioncircle.redis.cli.tool.ext.MemRdbVisitor.Tuple2Ex;

/**
 * @author Baoyi Chen
 */
public class MemRdbVisitor extends AbstractRdbVisitor implements Consumer<Tuple2Ex>, EventListener {

    private final MinHeap<Tuple2Ex> heap;

    public MemRdbVisitor(Replicator replicator,
                         File out,
                         List<Long> db,
                         List<String> regexs,
                         Long top,
                         List<Type> types,
                         Escape escape) throws Exception {
        super(replicator, out, db, regexs, top, types, escape);
        this.heap = new MinHeap<>(top == null ? -1 : top.intValue());
        this.heap.setConsumer(this);
        this.replicator.addEventListener(this);
    }

    @Override
    public void accept(Tuple2Ex tuple) {
        try {
            escape.encode(tuple.getV2(), out);
            out.write(' ');
            escape.encode(tuple.getV1(), out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof DummyKeyValuePair) {
            DummyKeyValuePair dkv = (DummyKeyValuePair) event;
            if (dkv.isContains()) heap.add(new Tuple2Ex(dkv.getValue(), dkv.getKey()));
        } else if (event instanceof PostFullSyncEvent) {
            for (Tuple2Ex tuple : heap.get()) accept(tuple);
        }
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }

    static class Tuple2Ex extends Tuple2<Long, byte[]> implements Comparable<Tuple2Ex> {

        public Tuple2Ex(Long v1, byte[] v2) {
            super(v1, v2);
        }

        public Tuple2Ex(Tuple2<Long, byte[]> rhs) {
            super(rhs);
        }

        @Override
        public int compareTo(Tuple2Ex that) {
            return Long.compare(this.getV1(), that.getV1());
        }
    }
}