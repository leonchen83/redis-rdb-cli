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

package com.moilioncircle.redis.cli.tool.net;

import com.moilioncircle.redis.cli.tool.io.BufferedOutputStream;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.cli.tool.util.Sockets;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.util.ByteBuilder;
import com.moilioncircle.redis.replicator.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import static com.moilioncircle.redis.replicator.Constants.COLON;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.MINUS;
import static com.moilioncircle.redis.replicator.Constants.PLUS;
import static com.moilioncircle.redis.replicator.Constants.STAR;

/**
 * @author Baoyi Chen
 */
public class Endpoint implements Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(Endpoint.class);
    
    private static final int BUFFER = 64 * 1024;
    private static final byte[] AUTH = "auth".getBytes();
    private static final byte[] PING = "ping".getBytes();
    private static final byte[] SELECT = "select".getBytes();
    
    private int count = 0;
    private final int pipe;
    private final Socket socket;
    private final OutputStream out;
    private final RedisInputStream in;
    
    public Endpoint(String host, int port, int db, int pipe, Configuration conf) {
        this.pipe = pipe;
        try {
            CliSocketFactory factory = new CliSocketFactory(conf);
            this.socket = factory.createSocket(host, port, conf.getConnectionTimeout());
            this.in = new RedisInputStream(this.socket.getInputStream(), BUFFER);
            this.out = new BufferedOutputStream(this.socket.getOutputStream(), BUFFER);
            if (conf.getAuthPassword() != null) {
                String r = send(AUTH, conf.getAuthPassword().getBytes());
                if (r != null) throw new RuntimeException(r);
            } else {
                String r = send(PING);
                if (r != null) throw new RuntimeException(r);
            }
            String r = send(SELECT, String.valueOf(db).getBytes());
            if (r != null) throw new RuntimeException(r);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public String send(byte[] command, byte[]... ary) {
        try {
            emit(out, command, ary);
            out.flush();
            return parse();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void batch(byte[] command, byte[]... args) {
        try {
            emit(out, command, args);
            count++;
            if (count == pipe) flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void flush() {
        try {
            if (count > 0) {
                OutputStreams.flush(out);
                for (int i = 0; i < count; i++) {
                    String r = parse();
                    if (r != null) logger.error(r);
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
    
    private String parse() throws IOException {
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
                    if (len == -1) return null;
                    in.skip(len);
                    return null;
                case COLON:
                    // RESP Integers
                    while (true) {
                        while (in.read() != '\r') {
                        }
                        if (in.read() == '\n') {
                            break;
                        }
                    }
                    // As integer
                    return null;
                case STAR:
                    // RESP Arrays
                    builder = ByteBuilder.allocate(32);
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
                    for (int i = 0; i < len; i++) {
                        parse();
                    }
                    return null;
                case PLUS:
                    // RESP Simple Strings
                    while (true) {
                        while (in.read() != '\r') {
                        }
                        if (in.read() == '\n') {
                            return null;
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
                            return Strings.toString(builder.array());
                        } else {
                            builder.put((byte) c);
                        }
                    }
                default:
                    throw new RuntimeException("expect [$,:,*,+,-] but: " + (char) c);
                
            }
        }
    }
}