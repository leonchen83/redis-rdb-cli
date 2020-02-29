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

package com.moilioncircle.redis.rdb.cli.glossary;

import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 * @see <a href="https://www.json.org/json-en.html">json</a>
 */
public class JsonEscape implements Escaper {
    
    private Escape escape;
    
    public JsonEscape(Escape escape) {
        this.escape = escape;
    }
    
    @Override
    public void encode(int b, OutputStream out, Configure configure) {
        b = b & 0xFF;
        if (b == '\n') {
            OutputStreams.write('\\', out);
            OutputStreams.write('n', out);
        } else if (b == '\r') {
            OutputStreams.write('\\', out);
            OutputStreams.write('r', out);
        } else if (b == '\t') {
            OutputStreams.write('\\', out);
            OutputStreams.write('t', out);
        } else if (b == '\b') {
            OutputStreams.write('\\', out);
            OutputStreams.write('b', out);
        } else if (b == '\f') {
            OutputStreams.write('\\', out);
            OutputStreams.write('f', out);
        } else if (b == '\\') {
            OutputStreams.write('\\', out);
            OutputStreams.write('\\', out);
        } else if (b == '/') {
            OutputStreams.write('\\', out);
            OutputStreams.write('/', out);
        } else if (b == '"') {
            OutputStreams.write('\\', out);
            OutputStreams.write('"', out);
        } else {
            escape.encode(b, out, configure);
        }
    }
    
    @Override
    public void encode(byte[] bytes, int off, int len, OutputStream out, Configure configure) {
        if (bytes == null) return;
        for (int i = off; i < off + len; i++) {
            encode(bytes[i], out, configure);
        }
    }

    @Override
    public void encode(byte[] bytes, OutputStream out, Configure configure) {
        encode(bytes, 0, bytes.length, out, configure);
    }

    @Override
    public void encode(long value, OutputStream out, Configure configure) {
        encode(String.valueOf(value).getBytes(), out, configure);
    }

    @Override
    public void encode(double value, OutputStream out, Configure configure) {
        encode(String.valueOf(value).getBytes(), out, configure);
    }
}
