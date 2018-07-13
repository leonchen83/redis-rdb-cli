package com.moilioncircle.redis.cli.tool.ext.rmt;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.DumpRawByteListener;
import com.moilioncircle.redis.cli.tool.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.net.Endpoint;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class MigrateRdbVisitor extends AbstractRdbVisitor implements EventListener {

    private static final byte[] ZERO = "0".getBytes();
    private static final byte[] RESTORE = "restore".getBytes();
    private static final byte[] REPLACE = "replace".getBytes();

    private Endpoint endpoint;
    private final RedisURI uri;
    private final boolean replace;
    private final Configuration configuration;

    public MigrateRdbVisitor(Replicator replicator, Configure configure, String uri, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) throws Exception {
        super(replicator, configure, db, regexs, types);
        this.replace = replace;
        this.uri = new RedisURI(uri);
        this.replicator.addEventListener(this);
        this.configuration = configure.merge(Configuration.valueOf(this.uri));
        this.replicator.addCloseListener(e -> Endpoint.closeQuietly(this.endpoint));
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            Endpoint.closeQuietly(this.endpoint);
            this.endpoint = new Endpoint(this.uri.getHost(), this.uri.getPort(), 0, configuration, configure);
        }
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        DB db = super.applySelectDB(in, version);
        endpoint.select((int) db.getDbNumber());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyString(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplySet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSet2(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHash(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHashZipMap(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyListZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplySetIntSet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSetZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHashZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyListQuickList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule2(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
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
        final byte[] fex = ex;
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        endpoint.send(out -> {
            OutputStreams.write(RESTORE, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            OutputStreams.call(() -> {
                Escape.REDIS.encode(key, out, configure);
                return null;
            });
            OutputStreams.write((byte) '"', out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write(fex, out);
            OutputStreams.write((byte) ' ', out);
            OutputStreams.write((byte) '"', out);
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, out, Escape.REDIS, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            OutputStreams.write((byte) '"', out);
            if (replace) {
                OutputStreams.write((byte) ' ', out);
                OutputStreams.write(REPLACE, out);
            }
            OutputStreams.write((byte) '\n', out);
        }, key);
        return context.valueOf(new DummyKeyValuePair());
    }
}
