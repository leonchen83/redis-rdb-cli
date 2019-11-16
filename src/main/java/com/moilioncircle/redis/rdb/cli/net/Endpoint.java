/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.rdb.cli.net;

import static com.moilioncircle.redis.replicator.Constants.COLON;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.MINUS;
import static com.moilioncircle.redis.replicator.Constants.PLUS;
import static com.moilioncircle.redis.replicator.Constants.STAR;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.io.BufferedOutputStream;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.rdb.cli.util.Sockets;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.net.RedisSocketFactory;
import com.moilioncircle.redis.replicator.util.ByteBuilder;

/**
 * @author Baoyi Chen
 */
public class Endpoint implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(Endpoint.class);
    
    private static final int BUFFER = 64 * 1024;
    private static final byte[] AUTH = "auth".getBytes();
    private static final byte[] PING = "ping".getBytes();
    private static final byte[] SELECT = "select".getBytes();
    
    private int db;
    private int count = 0;
    private final int pipe;
    private final int port;
    private final String host;
    private final Socket socket;
    private final String address;
    private final OutputStream out;
    private final Configuration conf;
    private final boolean statistics;
    private final Configure configure;
    private final RedisInputStream in;

    private final Monitor monitor;
    
    public Endpoint(String host, int port, Configuration conf, Configure configure) {
        this(host, port, 0, 1, false, conf, configure);
    }
    
    public Endpoint(String host, int port, int db, int pipe, boolean statistics, Configuration conf, Configure configure) {
        this.host = host;
        this.port = port;
        this.pipe = pipe;
        this.conf = conf;
        this.configure = configure;
        this.statistics = statistics;
        this.monitor = MonitorFactory.getMonitor("endpoint_statistics", configure.getMetricEndpointInstance());
        try {
            RedisSocketFactory factory = new RedisSocketFactory(conf);
            this.socket = factory.createSocket(host, port, conf.getConnectionTimeout());
            this.in = new RedisInputStream(this.socket.getInputStream(), BUFFER);
            this.out = new BufferedOutputStream(this.socket.getOutputStream(), BUFFER);
            if (conf.getAuthPassword() != null) {
                RedisObject r = send(AUTH, conf.getAuthPassword().getBytes());
                if (r != null && r.type.isError()) throw new RuntimeException(r.getString());
            } else {
                RedisObject r = send(PING);
                if (r != null && r.type.isError()) throw new RuntimeException(r.getString());
            }
            RedisObject r = send(SELECT, String.valueOf(db).getBytes());
            if (r != null && r.type.isError()) throw new RuntimeException(r.getString());
            this.db = db;
            this.address = address(socket).replaceAll("\\.", "_");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public String address(Socket socket) {
        Objects.requireNonNull(socket);
        InetSocketAddress la = (InetSocketAddress) socket.getLocalSocketAddress();
        InetSocketAddress ra = (InetSocketAddress) socket.getRemoteSocketAddress();
        StringBuilder builder = new StringBuilder();
        builder.append("[la=");
        if (la != null) {
            builder.append(la.toString());
        } else {
            builder.append("N/A");
        }
        builder.append(",ra=");
        if (ra != null) {
            builder.append(ra.toString());
        } else {
            builder.append("N/A");
        }
        builder.append("]");
        return builder.toString();
    }
    
    public int getDB() {
        return db;
    }
    
    public RedisObject send(byte[] command, byte[]... ary) {
        try {
            emit(out, command, ary);
            out.flush();
            return parse();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void select(boolean force, int db) {
        batch(force, SELECT, String.valueOf(db).getBytes());
        this.db = db;
    }
    
    public void batch(boolean force, byte[] command, byte[]... args) {
        try {
            emit(out, command, args);
            if (force) out.flush();
            count++;
            if (count == pipe) flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void flushQuietly() {
        try {
            flush();
        } catch (Throwable e) {
            logger.error("failed to flush.", e);
        }
    }
    
    public void flush() {
        try {
            if (count > 0) {
                int temp = count;
                long mark = System.nanoTime();
                OutputStreams.flush(out);
                for (int i = 0; i < count; i++) {
                    RedisObject r = parse();
                    if (r != null && r.type.isError()) {
                        logger.error(r.getString());
                        if (statistics) monitor.add("send_err_" + address, 1);
                    } else {
                        if (statistics) monitor.add("send_suc_" + address, 1);
                    }
                }
                count = 0;
                if (statistics) monitor.add("send_" + address, temp, System.nanoTime() - mark);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void close() throws IOException {
        Sockets.closeQuietly(in);
        Sockets.closeQuietly(out);
        Sockets.closeQuietly(socket);
    }
    
    public static void close(Endpoint endpoint) {
        if (endpoint == null) return;
        try {
            endpoint.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void closeQuietly(Endpoint endpoint) {
        if (endpoint == null) return;
        try {
            endpoint.close();
        } catch (Throwable e) {
        }
    }
    
    public static Endpoint valueOf(Endpoint endpoint) {
        closeQuietly(endpoint);
        return new Endpoint(endpoint.host, endpoint.port, endpoint.db, endpoint.pipe, endpoint.statistics, endpoint.conf, endpoint.configure);
    }
    
    private void emit(OutputStream out, byte[] command, byte[]... ary) throws IOException {
        out.write(STAR);
        out.write(String.valueOf(ary.length + 1).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(DOLLAR);
        out.write(String.valueOf(command.length).getBytes());
        out.write('\r');
        out.write('\n');
        out.write(command);
        out.write('\r');
        out.write('\n');
        for (final byte[] arg : ary) {
            out.write(DOLLAR);
            out.write(String.valueOf(arg.length).getBytes());
            out.write('\r');
            out.write('\n');
            out.write(arg);
            out.write('\r');
            out.write('\n');
        }
    }
    
    private RedisObject parse() throws IOException {
        while (true) {
            int c = in.read();
            switch (c) {
                case DOLLAR:
                    // RESP Bulk Strings
                    ByteBuilder builder = ByteBuilder.allocate(32);
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
                    if (len == -1) return new RedisObject(RedisObject.Type.NULL, null);
                    return new RedisObject(RedisObject.Type.BULK, in.readBytes(len).first());
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
                    return new RedisObject(RedisObject.Type.NUMBER, Long.parseLong(builder.toString()));
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
                    if (len == -1) return new RedisObject(RedisObject.Type.NULL, null);
                    RedisObject[] ary = new RedisObject[(int) len];
                    for (int i = 0; i < len; i++) {
                        RedisObject obj = parse();
                        ary[i] = obj;
                    }
                    return new RedisObject(RedisObject.Type.ARRAY, ary);
                case PLUS:
                    // RESP Simple Strings
                    builder = ByteBuilder.allocate(128);
                    while (true) {
                        while ((c = in.read()) != '\r') {
                            builder.put((byte) c);
                        }
                        if ((c = in.read()) == '\n') {
                            return new RedisObject(RedisObject.Type.STRING, builder.array());
                        } else {
                            builder.put((byte) c);
                        }
                    }
                case MINUS:
                    // RESP Errors
                    builder = ByteBuilder.allocate(32);
                    while (true) {
                        while ((c = in.read()) != '\r') {
                            builder.put((byte) c);
                        }
                        if ((c = in.read()) == '\n') {
                            return new RedisObject(RedisObject.Type.ERR, builder.array());
                        } else {
                            builder.put((byte) c);
                        }
                    }
                default:
                    throw new RuntimeException("expect [$,:,*,+,-] but: " + (char) c);
    
            }
        }
    }
    
    public static class RedisObject {
        public Type type;
        public Object object;
        
        public RedisObject(Type type, Object object) {
            this.type = type;
            this.object = object;
        }
        
        public String getString() {
            byte[] b = getBytes();
            return b == null ? null : new String(b);
        }
    
        public byte[] getBytes() {
            if (type.isString() || type.isError()) {
                byte[] bytes = (byte[]) object;
                return bytes;
            }
            return null;
        }
        
        public enum Type {
            ARRAY, NUMBER, STRING, BULK, ERR, NULL;
            
            public boolean isString() {
                return this == BULK || this == STRING;
            }
            
            public boolean isArray() {
                return this == ARRAY;
            }
            
            public boolean isNumber() {
                return this == NUMBER;
            }
            
            public boolean isError() {
                return this == ERR;
            }
            
            public boolean isNull() {
                return this == NULL;
            }
        }
    }
}