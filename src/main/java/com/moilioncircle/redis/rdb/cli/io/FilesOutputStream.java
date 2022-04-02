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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.util.type.Tuple3;

/**
 * @author Baoyi Chen
 */
public class FilesOutputStream extends OutputStream {

    private byte[] key;

    private final Set<CRCOutputStream> set = new HashSet<>();
    private final Map<Short, CRCOutputStream> map = new HashMap<>(32768);

    public FilesOutputStream(File path, List<String> lines, Configure configure) {
        Function<Tuple3<String, Integer, String>, CRCOutputStream> mapper = t -> {
            File file = Paths.get(path.getAbsolutePath(), t.getV3() + ".rdb").toFile();
            return Outputs.newCRCOutput(file, configure.getOutputBufferSize());
        };
        NodeConfParser.parse(lines, set, map, mapper);
    
        if (map.size() != 16384) {
            throw new UnsupportedOperationException("slots size : " + map.size() + ", expected 16384.");
        }
    }

    public void shard(byte[] key) {
        this.key = key;
    }

    @Override
    public void write(int b) throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.write(b);
            }
        } else {
            map.get(NodeConfParser.slot(key)).write(b);
        }
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.write(b, off, len);
            }
        } else {
            map.get(NodeConfParser.slot(key)).write(b, off, len);
        }
    }

    public void flush() throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.flush();
            }
        } else {
            map.get(NodeConfParser.slot(key)).flush();
        }
    }

    public void close() throws IOException {
        for (OutputStream out : set) {
            out.close();
        }
    }

    public void writeCRC() {
        for (CRCOutputStream out : set) {
            Outputs.writeQuietly(0xFF, out);
            Outputs.writeQuietly(out.getCRC64(), out);
        }
    }
}
