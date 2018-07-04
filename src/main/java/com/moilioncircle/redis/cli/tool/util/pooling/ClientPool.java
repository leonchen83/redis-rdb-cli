package com.moilioncircle.redis.cli.tool.util.pooling;

import cn.nextop.lite.pool.Pool;
import cn.nextop.lite.pool.PoolBuilder;
import cn.nextop.lite.pool.PoolValidation;
import cn.nextop.lite.pool.glossary.Lifecyclet;
import com.moilioncircle.redis.cli.tool.util.Closes;
import com.moilioncircle.redis.replicator.util.Strings;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.atomic.AtomicBoolean;

import static cn.nextop.lite.pool.PoolValidation.ACQUIRE;
import static cn.nextop.lite.pool.PoolValidation.PULSE;
import static cn.nextop.lite.pool.PoolValidation.RELEASE;
import static redis.clients.jedis.Protocol.Command.AUTH;
import static redis.clients.jedis.Protocol.Command.RESTORE;
import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author Baoyi Chen
 */
public class ClientPool {

    public static Pool<Client> create(String host, int port, String password) {
        Pool<Client> pool = new PoolBuilder<Client>().validator(e -> !e.broken.get()).supplier(() -> {
            Client client = new Client(host, port, 10000, 10000);
            if (password != null) {
                String r = client.send(AUTH, password.getBytes());
                if (r != null) System.out.println(r);
            }
            return client;
        }).consumer(e -> {
            Closes.closeQuietly(e);
        }).validation(new PoolValidation((byte) (RELEASE | ACQUIRE | PULSE))).build("jedis.pool");
        Lifecyclet.start(pool);
        return pool;
    }

    public static class Client extends redis.clients.jedis.Client {

        private AtomicBoolean broken = new AtomicBoolean(false);

        public Client(final String host, final int port, int timeout, int soTimeout) {
            super(host, port);
            setConnectionTimeout(timeout);
            setSoTimeout(soTimeout);
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
