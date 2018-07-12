package com.moilioncircle.redis.cli.tool.util.pooling;

import cn.nextop.lite.pool.Pool;
import cn.nextop.lite.pool.PoolBuilder;
import cn.nextop.lite.pool.PoolValidation;
import cn.nextop.lite.pool.glossary.Lifecyclet;
import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.util.Sockets;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.cmd.RedisCodec;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.net.RedisSocketFactory;
import com.moilioncircle.redis.replicator.util.ByteBuilder;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static cn.nextop.lite.pool.PoolValidation.ACQUIRE;
import static cn.nextop.lite.pool.PoolValidation.PULSE;
import static cn.nextop.lite.pool.PoolValidation.RELEASE;
import static com.moilioncircle.redis.cli.tool.util.pooling.SocketPool.Socket;
import static com.moilioncircle.redis.cli.tool.util.pooling.SocketPool.Socket.closeQuietly;
import static com.moilioncircle.redis.replicator.Constants.COLON;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.MINUS;
import static com.moilioncircle.redis.replicator.Constants.PLUS;
import static com.moilioncircle.redis.replicator.Constants.STAR;

/**
 * @author Baoyi Chen
 */
public class SocketPool implements Consumer<Socket>, Supplier<Socket>, Predicate<Socket> {

    private final int port;
    private final String host;
    private final Configuration conf;

    private SocketPool(String host, int port, Configuration conf) {
        this.host = host;
        this.port = port;
        this.conf = conf;
    }

    public static Pool<SocketPool.Socket> create(String host, int port, Configuration conf) {
        SocketPool sp = new SocketPool(host, port, conf);
        PoolValidation pv = new PoolValidation((byte) (RELEASE | ACQUIRE | PULSE));
        PoolBuilder<Socket> builder = new PoolBuilder<>();
        builder.validator(sp).supplier(sp).consumer(sp).validation(pv);
        return Lifecyclet.start(builder.build("redis.pool"));
    }

    @Override
    public void accept(Socket socket) {
        closeQuietly(socket);
    }

    @Override
    public boolean test(Socket socket) {
        return !socket.broken.get();
    }

    @Override
    public Socket get() {
        return new Socket(host, port, conf);
    }

    public static class Socket implements Closeable {

        private static final byte[] AUTH = "auth".getBytes();
        private static final byte[] PING = "ping".getBytes();
        private static final byte[] SELECT = "select".getBytes();
        private static final byte[] EXPIREAT = "expireat".getBytes();

        private AtomicBoolean broken = new AtomicBoolean(false);

        private final InputStream in;
        private final OutputStream out;
        private final java.net.Socket socket;
        private final RedisCodec codec = new RedisCodec();

        public Socket(final String host, final int port, final Configuration conf) {
            try {
                RedisSocketFactory factory = new RedisSocketFactory(conf);
                this.socket = factory.createSocket(host, port, conf.getConnectionTimeout());
                this.in = this.socket.getInputStream();
                this.out = this.socket.getOutputStream();
                if (conf.getAuthPassword() != null) {
                    String r = auth(conf.getAuthPassword());
                    if (r != null) throw new IOException(r);
                } else {
                    String r = ping();
                    if (r != null) throw new IOException(r);
                }
            } catch (IOException e) {
                throw new AssertionError(e.getMessage(), e);
            }
        }

        public String select(int db) {
            return send(out -> {
                try {
                    out.write(SELECT);
                    out.write(' ');
                    out.write(String.valueOf(db).getBytes());
                    out.write('\n');
                    out.flush();
                } catch (IOException e) {
                    throw new AssertionError(e.getMessage(), e);
                }
            });
        }

        public String auth(String password) {
            return send(out -> {
                try {
                    out.write(AUTH);
                    out.write(' ');
                    out.write(password.getBytes());
                    out.write('\n');
                    out.flush();
                } catch (IOException e) {
                    throw new AssertionError(e.getMessage(), e);
                }
            });
        }

        public String expireat(byte[] key, long ms, Configure configure) {
            return send(out -> {
                try {
                    out.write(EXPIREAT);
                    out.write(' ');
                    Escape.REDIS.encode(key, out, configure);
                    out.write(' ');
                    out.write(String.valueOf(ms).getBytes());
                    out.write('\n');
                    out.flush();
                } catch (IOException e) {
                    throw new AssertionError(e.getMessage(), e);
                }
            });
        }

        public String ping() {
            return send(out -> {
                try {
                    out.write(PING);
                    out.write('\n');
                    out.flush();
                } catch (IOException e) {
                    throw new AssertionError(e.getMessage(), e);
                }
            });
        }

        public String send(Consumer<OutputStream> consumer) {
            try {
                consumer.accept(out);
                parse(new RedisInputStream(in));
                return null;
            } catch (UncheckedIOException | IOException e) {
                broken.set(true);
                throw new AssertionError(e.getMessage(), e);
            } catch (ReplyException e) {
                return e.getMessage();
            }
        }

        public String send(Configure configure, byte[] command, byte[]... ary) {
            try {
                emit(command, ary, configure);
                parse(new RedisInputStream(in));
                return null;
            } catch (UncheckedIOException | IOException e) {
                broken.set(true);
                throw new AssertionError(e.getMessage(), e);
            } catch (ReplyException e) {
                return e.getMessage();
            }
        }

        private void emit(byte[] command, byte[][] ary, Configure configure) throws IOException {
            out.write(STAR);
            out.write(String.valueOf(ary.length + 1).getBytes());
            out.write('\r');
            out.write('\n');
            out.write(DOLLAR);
            byte[] c = Escape.REDIS.encode(command, configure);
            out.write(String.valueOf(c.length).getBytes());
            out.write('\r');
            out.write('\n');
            out.write(c);
            out.write('\r');
            out.write('\n');
            for (final byte[] arg : ary) {
                out.write(DOLLAR);
                byte[] a = Escape.REDIS.encode(arg, configure);
                out.write(String.valueOf(a.length).getBytes());
                out.write('\r');
                out.write('\n');
                out.write(a);
                out.write('\r');
                out.write('\n');
            }
            out.flush();
        }

        @Override
        public void close() throws IOException {
            Sockets.closeQuietly(in);
            Sockets.closeQuietly(out);
            Sockets.closeQuietly(socket);
        }

        public Object parse(RedisInputStream in) throws IOException {
            while (true) {
                int c = in.read();
                switch (c) {
                    case DOLLAR:
                        // RESP Bulk Strings
                        ByteBuilder builder = ByteBuilder.allocate(128);
                        while (true) {
                            while ((c = in.read()) != '\r') {
                                builder.put((byte) c);
                            }
                            if ((c = in.read()) == '\n') {
                                break;
                            } else {
                                builder.put((byte) c);
                            }
                        }
                        long len = Long.parseLong(builder.toString());
                        if (len == -1) return null;
                        byte[] bytes = in.readBytes(len).first();
                        bytes = codec.decode(bytes);
                        in.skip(2);
                        return bytes;
                    case COLON:
                        // RESP Integers
                        builder = ByteBuilder.allocate(128);
                        while (true) {
                            while ((c = in.read()) != '\r') {
                                builder.put((byte) c);
                            }
                            if ((c = in.read()) == '\n') {
                                break;
                            } else {
                                builder.put((byte) c);
                            }
                        }
                        // As integer
                        return Long.parseLong(builder.toString());
                    case STAR:
                        // RESP Arrays
                        builder = ByteBuilder.allocate(128);
                        while (true) {
                            while ((c = in.read()) != '\r') {
                                builder.put((byte) c);
                            }
                            if ((c = in.read()) == '\n') {
                                break;
                            } else {
                                builder.put((byte) c);
                            }
                        }
                        len = Long.parseLong(builder.toString());
                        if (len == -1) return null;
                        Object[] ary = new Object[(int) len];
                        for (int i = 0; i < len; i++) {
                            Object obj = parse(in);
                            ary[i] = obj;
                        }
                        return ary;
                    case PLUS:
                        // RESP Simple Strings
                        builder = ByteBuilder.allocate(128);
                        while (true) {
                            while ((c = in.read()) != '\r') {
                                builder.put((byte) c);
                            }
                            if ((c = in.read()) == '\n') {
                                return codec.decode(builder.array());
                            } else {
                                builder.put((byte) c);
                            }
                        }
                    case MINUS:
                        // RESP Errors
                        builder = ByteBuilder.allocate(128);
                        while (true) {
                            while ((c = in.read()) != '\r') {
                                builder.put((byte) c);
                            }
                            if ((c = in.read()) == '\n') {
                                throw new ReplyException(Strings.toString(codec.decode(builder.array())));
                            } else {
                                builder.put((byte) c);
                            }
                        }
                    default:
                        throw new AssertionError("expect [$,:,*,+,-] but: " + (char) c);

                }
            }
        }

        private static final class ReplyException extends RuntimeException {
            public ReplyException(String message) {
                super(message);
            }
        }

        public static void close(Socket socket) {
            if (socket == null) return;
            try {
                socket.close();
            } catch (Throwable txt) {
                throw new RuntimeException(txt);
            }
        }

        public static void closeQuietly(Socket socket) {
            if (socket == null) return;
            try {
                socket.close();
            } catch (Throwable t) {
            }
        }
    }
}
