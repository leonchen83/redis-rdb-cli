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

package com.moilioncircle.redis.rdb.cli.io;

import static com.moilioncircle.redis.rdb.cli.util.Iterators.iterator;
import static java.io.File.createTempFile;
import static java.nio.ByteBuffer.allocate;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.util.Iterator;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;
import com.moilioncircle.redis.replicator.io.ByteBufferOutputStream;

/**
 * @author Baoyi Chen
 */
public class LayeredOutputStream extends OutputStream {
    
    protected static final ByteBuffer EMPTY = allocate(0);
    
    protected static final File TEMP_DIR = new File(System.getProperty("java.io.tmpdir"));
    protected static final OpenOption[] OPTIONS = new OpenOption[]{READ, WRITE, TRUNCATE_EXISTING, CREATE, DELETE_ON_CLOSE};
    
    protected long size;
    protected final int max;
    protected final int init;
    protected FileChannel file;
    protected final String path;
    protected final String prefix;
    protected ByteBufferOutputStream buffer;
    
    public LayeredOutputStream(Configure configure) {
        this.init = configure.getOutputBufferSize();
        this.max = configure.getMaxOutputBufferSize();
        this.path = configure.getTempFilePath();
        this.prefix = configure.getTempFilePrefix();
        this.buffer = new ByteBufferOutputStream(init);
    }
    
    public LayeredOutputStream(int init, int max) {
        this.init = init;
        this.max = max;
        this.path = null;
        this.prefix = "rct";
        this.buffer = new ByteBufferOutputStream(init);
    }
    
    @Override
    public void write(int b) throws IOException {
        if (this.buffer.size() + 1 < max) {
            this.buffer.write(b);
        } else {
            flushToFile();
            this.buffer.write(b);
        }
        size++;
    }
    
    @Override
    public void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }
    
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        if (this.buffer.size() + len < max) {
            this.buffer.write(b, off, len);
        } else {
            flushToFile();
            if (len < max) {
                this.buffer.write(b, off, len);
            } else {
                int tmp = len;
                while (tmp >= max) {
                    this.buffer.write(b, off, max);
                    tmp -= max;
                    off += max;
                    flushToFile();
                }
                if (tmp > 0) {
                    this.buffer.write(b, off, tmp);
                }
            }
        }
        size += len;
    }
    
    @Override
    public void flush() throws IOException {
        this.buffer.flush();
        if (file != null) file.force(false);
    }
    
    @Override
    public void close() throws IOException {
        this.buffer.close();
        if (file != null) this.file.close();
    }
    
    public long size() {
        return size;
    }
    
    public ByteBuffers toByteBuffers() {
        Iterator<ByteBuffer> it;
        if (size < max) {
            it = iterator(buffer.toByteBuffer());
        } else {
            it = new Iter();
        }
        ByteBuffers buffers = new ByteBuffers(this);
        buffers.setBuffers(it);
        buffers.setSize(this.size);
        return buffers;
    }
    
    private void flushToFile() throws IOException {
        if (file == null) {
            File path;
            if (this.path == null) {
                path = TEMP_DIR;
            } else {
                path = new File(this.path);
            }
            File temp = createTempFile(prefix, null, path);
            file = open(temp.toPath(), OPTIONS);
        }
        ByteBuffer buf = this.buffer.toByteBuffer();
        while (buf.remaining() >= init) {
            ByteBuffer tmp = (ByteBuffer) buf.slice().limit(init);
            buf.position(buf.position() + init);
            file.write(tmp);
        }
        if (buf.hasRemaining()) file.write(buf);
        this.buffer.reset();
    }
    
    private class Iter implements Iterator<ByteBuffer> {
        
        private Iter() {
            try {
                flushToFile();
                file.position(0);
            } catch (IOException e) {
            }
        }
        
        @Override
        public boolean hasNext() {
            try {
                return file.position() < size;
            } catch (IOException e) {
                return false;
            }
        }
        
        @Override
        public ByteBuffer next() {
            try {
                ByteBuffer r = allocate(init);
                file.read(r);
                return (ByteBuffer) r.flip();
            } catch (IOException e) {
                return EMPTY;
            }
        }
    }
}
