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

package com.moilioncircle.redis.rdb.cli.ext;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.io.CRCOutputStream;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.io.RawByteListener;

/**
 * @author Baoyi Chen
 */
public class DumpRawByteListener implements RawByteListener, Closeable {
    private final int version;
    private final CRCOutputStream out;
    private final Replicator replicator;
    
    public DumpRawByteListener(Replicator replicator, int version, OutputStream out, Escaper escaper) {
        this.version = version;
        this.replicator = replicator;
        this.replicator.addRawByteListener(this);
        this.out = new CRCOutputStream(out, escaper);
    }
    
    public void write(byte type) throws IOException {
        this.out.write(type);
    }
    
    @Override
    public void handle(byte... rawBytes) {
        OutputStreams.write(rawBytes, out);
    }
    
    @Override
    public void close() throws IOException {
        this.replicator.removeRawByteListener(this);
        this.out.write((byte) version);
        this.out.write((byte) 0x00);
        this.out.write(this.out.getCRC64());
    }
}