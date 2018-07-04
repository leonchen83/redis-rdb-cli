package com.moilioncircle.redis.cli.tool.ext;

import cn.nextop.lite.pool.Pool;
import cn.nextop.lite.pool.glossary.Lifecyclet;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.util.pooling.ClientPool;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.util.Concurrents;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static redis.clients.jedis.Protocol.Command.SELECT;
import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author Baoyi Chen
 */
public class MigrateRdbVisitor extends AbstractRdbVisitor implements EventListener {

    private final RedisURI uri;
    private final boolean replace;
    private final Pool<ClientPool.Client> pool;
    private final AtomicInteger dbnum = new AtomicInteger(-1);
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public MigrateRdbVisitor(Replicator replicator, String uri, List<Long> db, List<String> regexs, List<Type> types, boolean replace) throws Exception {
        super(replicator, db, regexs, types);
        this.replace = replace;
        this.uri = new RedisURI(uri);
        Configuration config = Configuration.valueOf(this.uri);
        this.pool = ClientPool.create(this.uri.getHost(), this.uri.getPort(), config.getAuthPassword());
        this.replicator.addCloseListener(e -> {
            Concurrents.terminateQuietly(executor, 30, TimeUnit.SECONDS);
            Lifecyclet.stopQuietly(this.pool);
        });
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        executor.submit(() -> {
            ClientPool.Client target = null;
            try {
                target = pool.acquire();
                if (target == null) target = pool.acquire();
                if (target != null) {
                    if (event instanceof DumpKeyValuePair) {
                        DumpKeyValuePair dkv = (DumpKeyValuePair) event;
                        // Step1: select db
                        DB db = dkv.getDb();
                        int index;
                        if (db != null && (index = (int) db.getDbNumber()) != dbnum.get()) {
                            String r = target.send(SELECT, toByteArray(index));
                            if (r != null) System.out.println(r);
                            dbnum.set(index);
                        }

                        // Step2: restore dump data
                        if (dkv.getExpiredMs() == null) {
                            String r = target.restore(dkv.getKey(), 0L, dkv.getValue(), replace);
                            if (r != null) System.out.println(r);
                        } else {
                            long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                            if (ms <= 0) return;
                            String r = target.restore(dkv.getKey(), ms, dkv.getValue(), replace);
                            if (r != null) System.out.println(r);
                        }
                    }

                    if (event instanceof DefaultCommand) {
                        // Step3: sync aof command
                        DefaultCommand dc = (DefaultCommand) event;
                        String r = target.send(dc.getCommand(), dc.getArgs());
                        if (r != null) System.out.println(r);
                    }
                }
            } catch (Throwable e) {

            } finally {
                if (target != null) pool.release(target);
            }
        });
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
}
