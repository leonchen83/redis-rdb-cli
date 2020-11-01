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

package com.moilioncircle.redis.rdb.cli.ext.escape;

import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 */
public class RedisEscaper implements Escaper {
    
    private static final byte[] NUMERALS = new byte[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    
    private byte[] excludes;
    
    public RedisEscaper(byte... excludes) {
        this.excludes = excludes;
    }
    
    private boolean isContains(int b) {
        if (excludes == null || excludes.length == 0) return false;
        for (byte v : excludes) if (v == b) return true; return false;
    }
    
    @Override
    public void encode(int b, OutputStream out) {
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
        } else if (b == 7) {
            OutputStreams.write('\\', out);
            OutputStreams.write('a', out);
        } else if (b == 34 || b == 39 || b == 92 || b <= 32 || b >= 127 || isContains(b)) {
            OutputStreams.write('\\', out);
            OutputStreams.write('x', out);
            int ma = b >>> 4;
            int mi = b & 0xF;
            OutputStreams.write(NUMERALS[ma], out);
            OutputStreams.write(NUMERALS[mi], out);
        } else {
            OutputStreams.write(b, out);
        }
    }
    
    @Override
    public void encode(byte[] bytes, int off, int len, OutputStream out) {
        if (bytes == null) return;
        for (int i = off; i < off + len; i++) {
            encode(bytes[i], out);
        }
    }
}
