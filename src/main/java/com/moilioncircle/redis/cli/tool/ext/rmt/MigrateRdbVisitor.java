package com.moilioncircle.redis.cli.tool.ext.rmt;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.DumpRawByteListener;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static com.moilioncircle.redis.cli.tool.util.pooling.SocketPool.Socket;

/**
 * @author Baoyi Chen
 */
public class MigrateRdbVisitor extends AbstractRdbVisitor implements EventListener {

    private static final Logger logger = LoggerFactory.getLogger(MigrateRdbVisitor.class);

    private static final byte[] ZERO = "0".getBytes();
    private static final byte[] RESTORE = "restore".getBytes();
    private static final byte[] REPLACE = "replace".getBytes();

    private int db = 0;
    private int retry = 0;
    private Socket socket;
    private final RedisURI uri;
    private final boolean replace;
    private final Configuration configuration;

    public MigrateRdbVisitor(Replicator replicator, Configure configure, String uri, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) throws Exception {
        super(replicator, configure, db, regexs, types);
        this.replace = replace;
        this.uri = new RedisURI(uri);
        this.replicator.addEventListener(this);
        this.retry = configure.getMigrateRetryTime();
        this.replicator.addCloseListener(e -> Socket.closeQuietly(socket));
        this.configuration = configure.merge(Configuration.valueOf(this.uri));
    }

    protected String retry(Function<Socket, String> func, int times) {
        try {
            return func.apply(socket);
        } catch (Throwable e) {
            times--;
            if (times >= 0) {
                Socket.closeQuietly(socket);
                socket = new Socket(this.uri.getHost(), this.uri.getPort(), db, configuration);
                return retry(func, times);
            }
            throw e;
        }
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            Socket.closeQuietly(this.socket);
            this.socket = new Socket(this.uri.getHost(), this.uri.getPort(), 0, configuration);
        }
        if (event instanceof DefaultCommand) {
            DefaultCommand dc = (DefaultCommand) event;
            String r = retry(s -> s.send(configure, dc.getCommand(), dc.getArgs()), retry);
            if (r != null) logger.error("[{}] failed. reason:{}", Strings.toString(dc.getCommand()), r);
        }
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        this.db = (int) db.getDbNumber();
        String r = retry(s -> s.select((int) db.getDbNumber()), retry);
        if (r != null) logger.error("[select] {} failed. reason:{}", db.getDbNumber(), r);
        return db;
    }

    @Override
    public Event applyExpireTime(RedisInputStream in, DB db, int version) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTime(in, db, version);
        if (!kv.isContains() || kv.getKey() == null) return kv;
        String r = retry(s -> s.expireat(kv.getKey(), kv.getExpiredSeconds() * 1000, configure), retry);
        if (r != null) logger.error("[expireat] {} failed. reason:{}", Strings.toString(kv.getKey()), r);
        return kv;
    }

    @Override
    public Event applyExpireTimeMs(RedisInputStream in, DB db, int version) throws IOException {
        DummyKeyValuePair kv = (DummyKeyValuePair) super.applyExpireTimeMs(in, db, version);
        if (!kv.isContains() || kv.getKey() == null) return kv;
        String r = retry(s -> s.expireat(kv.getKey(), kv.getExpiredMs(), configure), retry);
        if (r != null) logger.error("[expireat] {} failed. reason:{}", Strings.toString(kv.getKey()), r);
        return kv;
    }

    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyString(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyList(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplySet(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyZSet(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyZSet2(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyHash(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyHashZipMap(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyListZipList(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplySetIntSet(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyZSetZipList(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyHashZipList(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyListQuickList(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyModule(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyModule2(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        String r = retry(s -> s.send(out -> {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                out.write(RESTORE);
                out.write(' ');
                Escape.REDIS.encode(key, out, configure);
                out.write(' ');
                out.write(ZERO);
                out.write(' ');
                replicator.addRawByteListener(listener);
                super.doApplyStreamListPacks(in, db, version, key, contains, type);
                replicator.removeRawByteListener(listener);
                if (replace) {
                    out.write(' ');
                    out.write(REPLACE);
                }
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }), retry);
        if (r != null)
            logger.error("[restore] {} failed. reason:{}", Strings.toString(key), r);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setDb(db);
        kv.setValueRdbType(type);
        kv.setKey(r != null ? null : key);
        kv.setContains(contains);
        return kv;
    }
}
