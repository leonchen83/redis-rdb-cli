/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.moilioncircle.redis.rdb.cli.io.LayeredOutputStream;

/**
 * @author Baoyi Chen
 */
public class ByteBuffers implements Closeable {
    private long size;
    private LayeredOutputStream out;
    private Iterator<ByteBuffer> buffers;
    
    public ByteBuffers(LayeredOutputStream out) {
        this.out = out;
    }
    
    private ByteBuffers(ByteBuffer buf) {
        this.size = buf.remaining();
        this.buffers = Iterators.iterator(buf);
    }
    
    public static ByteBuffers wrap(ByteBuffer buf) {
        return new ByteBuffers(buf);
    }
    
    public static ByteBuffers wrap(byte[] ary) {
        return new ByteBuffers(ByteBuffer.wrap(ary));
    }
    
    public long getSize() {
        return size;
    }
    
    public void setSize(long size) {
        this.size = size;
    }
    
    public Iterator<ByteBuffer> getBuffers() {
        return buffers;
    }
    
    public void setBuffers(Iterator<ByteBuffer> buffers) {
        this.buffers = buffers;
    }
    
    public void reset() {
        if (out == null) return;
        ByteBuffers that = out.toByteBuffers();
        this.size = that.size;
        this.buffers = that.buffers;
    }
    
    @Override
    public void close() {
        try {
            if (out != null) out.close();
        } catch (IOException ignore) {
        }
    }
}
