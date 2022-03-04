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

package com.moilioncircle.redis.rdb.cli.io;

import static com.moilioncircle.redis.rdb.cli.glossary.Guard.DRAIN;
import static com.moilioncircle.redis.rdb.cli.glossary.Guard.SAVE;

import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.glossary.Guard;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.util.ByteBuilder;

/**
 * @author Baoyi Chen
 */
public class GuardOutputStream extends OutputStream {
    
    private OutputStream out;
    private Guard guard = SAVE;
    private final ByteBuilder builder;
    
    
    public GuardOutputStream(int cap, OutputStream out) {
        this.out = out;
        this.builder = ByteBuilder.allocate(cap);
    }
    
    public void setGuard(Guard guard) {
        this.guard = guard;
    }
    
    public void reset(OutputStream out) {
        Outputs.closeQuietly(this.out);
        this.out = out;
        this.builder.clear();
    }
    
    @Override
    public void write(int b) throws IOException {
        if (guard == DRAIN) {
            if (out == null) return;
            if (builder.length() > 0) {
                byte[] ary = builder.array();
                out.write(ary);
                builder.clear();
            }
            out.write(b);
        } else if (guard == SAVE) {
            builder.put((byte) b);
        } else {
            if (builder.length() > 0) {
                builder.clear();
            }
        }
    }
    
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        for (int i = off; i < off + len; i++) write(b[i]);
    }
    
    public void flush() throws IOException {
        if (out != null) out.flush();
    }
    
    public void close() throws IOException {
        if (out != null) out.close();
    }
}
