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

package com.moilioncircle.redis.rdb.cli.glossary;

import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;

/**
 * @author Baoyi Chen
 */
public enum Escape implements Escaper {
    RAW("raw"),
    REDIS("redis");

    private String value;

    Escape(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    private static final byte[] NUMERALS = new byte[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static Escape parse(String escape) {
        if (escape == null) return RAW;
        switch (escape) {
            case "raw":
                return RAW;
            case "redis":
                return REDIS;
            default:
                throw new AssertionError("Unsupported escape '" + escape + "'");
        }
    }

    @Override
    public void encode(int b, OutputStream out, Configure configure) {
        b = b & 0xFF;
        switch (this) {
            case RAW:
                OutputStreams.write(b, out);
                break;
            case REDIS:
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
                } else if (b == 34 || b == 39 || b == 92 || b <= 32 || b >= 127 ||
                        b == configure.getDelimiter() || b == configure.getQuote()) {
                    // encode " ' \ unprintable and space
                    OutputStreams.write('\\', out);
                    OutputStreams.write('x', out);
                    int ma = b >>> 4;
                    int mi = b & 0xF;
                    OutputStreams.write(NUMERALS[ma], out);
                    OutputStreams.write(NUMERALS[mi], out);
                } else {
                    OutputStreams.write(b, out);
                }
                break;
        }
    }

    @Override
    public void encode(byte[] bytes, int off, int len, OutputStream out, Configure configure) {
        if (bytes == null) return;
        switch (this) {
            case RAW:
                OutputStreams.write(bytes, off, len, out);
                break;
            case REDIS:
                for (int i = off; i < off + len; i++) {
                    encode(bytes[i], out, configure);
                }
                break;
            default:
                throw new AssertionError(this);
        }
    }
}
