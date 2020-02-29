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

import static com.moilioncircle.redis.replicator.util.CRC64.crc64;
import static com.moilioncircle.redis.replicator.util.CRC64.longToByteArray;

import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.Escaper;

/**
 * @author Baoyi Chen
 */
public class CRCOutputStream extends OutputStream {
    
    private long checksum = 0L;
    private final Escaper escaper;
    private final OutputStream out;
    private final Configure configure;
    
    public CRCOutputStream(OutputStream out, Escaper escaper, Configure configure) {
        this.out = out;
        this.escaper = escaper;
        this.configure = configure;
    }
    
    public byte[] getCRC64() {
        return longToByteArray(checksum);
    }
    
    @Override
    public void write(int b) throws IOException {
        escaper.encode(b, out, configure);
        checksum = crc64(new byte[]{(byte) b}, checksum);
    }
    
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        escaper.encode(b, off, len, out, configure);
        checksum = crc64(b, off, len, checksum);
    }
    
    public void flush() throws IOException {
        out.flush();
    }
    
    public void close() throws IOException {
        out.close();
    }
}