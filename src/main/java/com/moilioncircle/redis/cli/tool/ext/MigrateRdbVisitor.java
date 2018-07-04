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
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteBuilder;
import com.moilioncircle.redis.replicator.util.Concurrents;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;
import static com.moilioncircle.redis.replicator.util.CRC64.crc64;
import static com.moilioncircle.redis.replicator.util.CRC64.longToByteArray;
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
                System.out.println(e.getMessage());
            } finally {
                if (target != null) pool.release(target);
            }
        });
    }
    
    private class DefaultRawByteListener implements RawByteListener {
        private final int version;
        private final ByteBuilder builder;
        
        private DefaultRawByteListener(byte type, int version) {
            this.builder = ByteBuilder.allocate(8192);
            this.builder.put(type);
            this.version = version;
        }
        
        @Override
        public void handle(byte... rawBytes) {
            for (byte b : rawBytes) this.builder.put(b);
        }
        
        public byte[] getBytes() {
            this.builder.put((byte) version);
            this.builder.put((byte) 0x00);
            byte[] bytes = this.builder.array();
            byte[] crc = longToByteArray(crc64(bytes));
            for (byte b : crc) {
                this.builder.put(b);
            }
            return this.builder.array();
        }
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadEncodedStringObject();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadDoubleValue();
            len--;
        }
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadBinaryDoubleValue();
            len--;
        }
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        while (len > 0) {
            skipParser.rdbLoadEncodedStringObject();
            skipParser.rdbLoadEncodedStringObject();
            len--;
        }
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        new SkipRdbParser(in).rdbLoadPlainStringObject();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipParser = new SkipRdbParser(in);
        long len = skipParser.rdbLoadLen().len;
        for (int i = 0; i < len; i++) {
            skipParser.rdbGenericLoadStringObject();
        }
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
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
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
        replicator.addRawByteListener(listener);
        SkipRdbParser skipRdbParser = new SkipRdbParser(in);
        skipRdbParser.rdbLoadCheckModuleValue();
        replicator.removeRawByteListener(listener);
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        DefaultRawByteListener listener = new DefaultRawByteListener((byte) type, version);
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
        DumpKeyValuePair kv = new DumpKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.getBytes());
        return kv;
    }
}
