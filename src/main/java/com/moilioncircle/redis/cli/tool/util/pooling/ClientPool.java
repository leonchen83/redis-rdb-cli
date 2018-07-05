package com.moilioncircle.redis.cli.tool.util.pooling;

import cn.nextop.lite.pool.Pool;
import cn.nextop.lite.pool.PoolBuilder;
import cn.nextop.lite.pool.PoolValidation;
import cn.nextop.lite.pool.glossary.Lifecyclet;
import com.moilioncircle.redis.cli.tool.util.Closes;
import com.moilioncircle.redis.replicator.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static cn.nextop.lite.pool.PoolValidation.ACQUIRE;
import static cn.nextop.lite.pool.PoolValidation.PULSE;
import static cn.nextop.lite.pool.PoolValidation.RELEASE;
import static com.moilioncircle.redis.cli.tool.util.pooling.ClientPool.Client;
import static redis.clients.jedis.Protocol.Command.AUTH;
import static redis.clients.jedis.Protocol.Command.PING;
import static redis.clients.jedis.Protocol.Command.RESTORE;
import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author Baoyi Chen
 */
public class ClientPool implements Consumer<Client>, Supplier<Client>, Predicate<Client> {

    private static final Logger logger = LoggerFactory.getLogger(ClientPool.class);

    private final int port;
    private final String host;
    private final int timeout;
    private final String password;

    private ClientPool(String host, int port, String password, int timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.password = password;
    }

    public static Pool<Client> create(String host, int port, String password, int timeout) {
        ClientPool cp = new ClientPool(host, port, password, timeout);
        PoolValidation pv = new PoolValidation((byte) (RELEASE | ACQUIRE | PULSE));
        PoolBuilder<Client> builder = new PoolBuilder<>();
        builder.validator(cp).supplier(cp).consumer(cp).validation(pv);
        Pool<Client> pool = builder.build("redis.pool");
        Lifecyclet.start(pool);
        return pool;
    }

    @Override
    public void accept(Client client) {
        Closes.closeQuietly(client);
    }

    @Override
    public boolean test(Client client) {
        return !client.broken.get();
    }

    @Override
    public Client get() {
        Client client = new Client(host, port, timeout);
        if (password != null) {
            String r = client.send(AUTH, password.getBytes());
            if (r != null) logger.error("auth[{}:{}] failed. reason:{}", host, port, r);
        } else {
            String r = client.send(PING);
            if (r != null) logger.error("ping[{}:{}] failed. reason:{}", host, port, r);
        }
        return client;
    }

    public static class Client extends redis.clients.jedis.Client {

        private AtomicBoolean broken = new AtomicBoolean(false);

        public Client(final String host, final int port, int timeout) {
            super(host, port);
            setSoTimeout(timeout);
            setConnectionTimeout(timeout);
        }

        public String send(Protocol.Command cmd, final byte[]... args) {
            try {
                sendCommand(cmd, args);
                getOne();
                return null;
            } catch (JedisConnectionException e) {
                broken.set(true);
                throw e;
            } catch (Throwable e) {
                return e.getMessage();
            }
        }

        public String send(final byte[] cmd, final byte[]... args) {
            return send(Protocol.Command.valueOf(Strings.toString(cmd).toUpperCase()), args);
        }

        public String restore(byte[] key, long expired, byte[] dumped, boolean replace) {
            if (!replace) {
                return send(RESTORE, key, toByteArray(expired), dumped);
            } else {
                return send(RESTORE, key, toByteArray(expired), dumped, "REPLACE".getBytes());
            }
        }
    }
}
