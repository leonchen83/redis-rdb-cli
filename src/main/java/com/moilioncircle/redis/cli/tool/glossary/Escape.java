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

package com.moilioncircle.redis.cli.tool.glossary;

import com.moilioncircle.redis.cli.tool.conf.Configure;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
public enum Escape {
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

    public void encode(int b, OutputStream out, Configure configure) throws IOException {
        b = b & 0xFF;
        switch (this) {
            case RAW:
                out.write(b);
                break;
            case REDIS:
                if (b == '\n') {
                    out.write('\\');
                    out.write('n');
                } else if (b == '\r') {
                    out.write('\\');
                    out.write('r');
                } else if (b == '\t') {
                    out.write('\\');
                    out.write('t');
                } else if (b == '\b') {
                    out.write('\\');
                    out.write('b');
                } else if (b == 7) {
                    out.write('\\');
                    out.write('a');
                } else if (b == 34 || b == 39 || b == 92 || b <= 32 || b >= 127 ||
                        b == configure.getDelimiter() || b == configure.getQuote()) {
                    // encode " ' \ unprintable and space
                    out.write('\\');
                    out.write('x');
                    int ma = b >>> 4;
                    int mi = b & 0xF;
                    out.write(NUMERALS[ma]);
                    out.write(NUMERALS[mi]);
                } else {
                    out.write(b);
                }
                break;
        }
    }

    public void encode(double value, OutputStream out, Configure configure) throws IOException {
        encode(String.valueOf(value).getBytes(), out, configure);
    }

    public void encode(byte[] bytes, OutputStream out, Configure configure) throws IOException {
        if (bytes == null) return;
        encode(bytes, 0, bytes.length, out, configure);
    }

    public void encode(byte[] bytes, int off, int len, OutputStream out, Configure configure) throws IOException {
        if (bytes == null) return;
        switch (this) {
            case RAW:
                out.write(bytes, off, len);
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
