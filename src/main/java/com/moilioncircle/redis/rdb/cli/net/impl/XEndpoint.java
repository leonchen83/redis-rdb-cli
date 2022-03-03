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

package com.moilioncircle.redis.rdb.cli.net.impl;

import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.AUTH;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.PING;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.SELECT;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.io.BufferedOutputStream;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.rdb.cli.net.AbstractEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.Protocol;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.rdb.cli.util.Sockets;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.net.RedisSocketFactory;

/**
 * @author Baoyi Chen
 */
public class XEndpoint extends AbstractEndpoint implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(XEndpoint.class);
    
    private static final int BUFFER = 64 * 1024;
    
    private int db;
    private int count = 0;
    private final int pipe;
    private final Socket socket;
    private final String address;
    private final OutputStream out;
    private final Protocol protocol;
    private final Configuration conf;
    private final boolean statistics;
    private final RedisInputStream in;

    private final Monitor monitor;
    
    public XEndpoint(String host, int port, Configuration conf) {
        this(host, port, 0, 1, false, conf);
    }
    
    public XEndpoint(String host, int port, int db, int pipe, boolean statistics, Configuration conf) {
        this.host = host;
        this.port = port;
        this.pipe = pipe;
        this.conf = conf;
        this.statistics = statistics;
        this.monitor = MonitorFactory.getMonitor("endpoint_statistics");
        try {
            RedisSocketFactory factory = new RedisSocketFactory(conf);
            this.socket = factory.createSocket(host, port, conf.getConnectionTimeout());
            this.in = new RedisInputStream(this.socket.getInputStream(), BUFFER);
            this.out = new BufferedOutputStream(this.socket.getOutputStream(), BUFFER);
            this.protocol = new Protocol(in, out);
            if (conf.getAuthPassword() != null) {
                RedisObject r = null;
                if (conf.getAuthUser() != null) {
                    // redis6 acl
                    r = send(AUTH, conf.getAuthUser().getBytes(), conf.getAuthPassword().getBytes());
                } else {
                    r = send(AUTH, conf.getAuthPassword().getBytes());
                }
                if (r != null && r.type.isError()) throw new RuntimeException(r.getString());
            } else {
                RedisObject r = send(PING);
                if (r != null && r.type.isError()) throw new RuntimeException(r.getString());
            }
            RedisObject r = send(SELECT, String.valueOf(db).getBytes());
            if (r != null && r.type.isError()) throw new RuntimeException(r.getString());
            this.db = db;
            this.address = this.toString().replaceAll("\\.", "_").replaceAll(":", "_");
            logger.debug("connected to {}:{}", host, port, db);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public String address(Socket socket) {
        Objects.requireNonNull(socket);
        InetSocketAddress ra = (InetSocketAddress) socket.getRemoteSocketAddress();
        StringBuilder builder = new StringBuilder();
        builder.append("[ra=");
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
            flush();
            protocol.emit(command, ary);
            out.flush();
            return protocol.parse();
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
            long mark = System.nanoTime();
            protocol.emit(command, args);
            if (force) {
                out.flush();
                if (statistics) monitor.add("send_" + address, 1, System.nanoTime() - mark);
            }
            count++;
            if (count == pipe) flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void batch(boolean force, ByteBuffers command, ByteBuffers... args) {
        try {
            long mark = System.nanoTime();
            protocol.emit(command, args);
            if (force) {
                out.flush();
                if (statistics) monitor.add("send_" + address, 1, System.nanoTime() - mark);
            }
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
            logger.error("failed to flush. host:{}, port:{}, reason:{}", host, port, e.getMessage());
        }
    }
    
    public void flush() {
        try {
            if (count > 0) {
                OutputStreams.flush(out);
                for (int i = 0; i < count; i++) {
                    RedisObject r = protocol.parse();
                    if (r != null && r.type.isError()) {
                        logger.error(r.getString());
                        if (statistics) monitor.add("failure_respond", 1);
                    } else {
                        if (statistics) monitor.add("success_respond", 1);
                    }
                }
                count = 0;
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
    
    public static void close(XEndpoint endpoint) {
        if (endpoint == null) {
            return;
        }
        try {
            endpoint.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "<" + host + ":" + port + ">";
    }
    
    public static void closeQuietly(XEndpoint endpoint) {
        if (endpoint == null) {
            return;
        }
        try {
            endpoint.close();
        } catch (Throwable e) {
        }
    }

    public static XEndpoint valueOfQuietly(XEndpoint endpoint, int db) {
        try {
            return valueOf(endpoint, db);
        } catch (Throwable e) {
            return null;
        }
    }

    public static XEndpoint valueOfQuietly(String host, int port, int db, XEndpoint endpoint) {
        try {
            return valueOf(host, port, db, endpoint);
        } catch (Throwable e) {
            return null;
        }
    }
    
    public static XEndpoint valueOf(XEndpoint endpoint, int db) {
        return valueOf(endpoint.host, endpoint.port, db, endpoint);
    }

    public static XEndpoint valueOf(String host, int port, int db, XEndpoint endpoint) {
        if (endpoint.statistics) {
            endpoint.monitor.add("reconnect_" + endpoint.address, 1);
        }
        closeQuietly(endpoint);
        XEndpoint v = new XEndpoint(host, port, db, endpoint.pipe, endpoint.statistics, endpoint.conf);
        v.setSlots(new ArrayList<>(endpoint.slots));
        return v;
    }
    
    public static DummyEndpoint toDummy(XEndpoint endpoint) {
        DummyEndpoint dummy = new DummyEndpoint(endpoint.getHost(), endpoint.getPort());
        dummy.setSlots(new ArrayList<>(endpoint.getSlots())); // copy
        return dummy;
    }
}