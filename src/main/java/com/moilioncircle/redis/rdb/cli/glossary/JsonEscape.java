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

    public final static int[] ESCAPES;

    static {
        int[] table = new int[128];
        for (int i = 0; i < 32; ++i) {
            table[i] = -1;
        }
        table['"'] = '"';
        table['\\'] = '\\';
        table[0x08] = 'b';
        table[0x09] = 't';
        table[0x0C] = 'f';
        table[0x0A] = 'n';
        table[0x0D] = 'r';
        ESCAPES = table;
    }

    public static final char[] HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public final static int SURR1_FIRST = 0xD800;
    public final static int SURR2_FIRST = 0xDC00;
    public final static int SURR1_LAST = 0xDBFF;
    public final static int SURR2_LAST = 0xDFFF;


    private Escape escape;

    public JsonEscape(Escape escape) {
        this.escape = escape;
    }

    @Override
    public void encode(int b, OutputStream out, Configure configure) {
        escape.encode(b, out, configure);
    }

    @Override
    public void encode(byte[] bytes, int off, int len, OutputStream out, Configure configure) {
        if (bytes == null) return;
        switch (escape) {
            case REDIS:
                escape.encode(bytes, off, len, out, configure);
                break;
            case RAW:
                encode(new String(bytes), off, len, out, configure);
                break;
        }
    }

    public void encode(String value, int off, int len, OutputStream out, Configure configure) {
        while (off < len) {
            int ch = value.charAt(off++);
            if (ch <= 0x7F) {
                if (ESCAPES[ch] == 0) {
                    OutputStreams.write((byte) ch, out);
                    continue;
                }
                int escape = ESCAPES[ch];
                if (escape > 0) {
                    OutputStreams.write('\\', out);
                    OutputStreams.write((byte) escape, out);
                } else {
                    // ctrl-char, 6-byte escape...
                    writeGenericEscape(ch, out);
                }
                continue;
            }
            if (ch <= 0x7FF) {
                OutputStreams.write((byte) (0xc0 | (ch >> 6)), out);
                OutputStreams.write((byte) (0x80 | (ch & 0x3f)), out);
            } else {
                writeMultiByteChar(ch, out);
            }
        }
    }

    private void writeGenericEscape(int ch, OutputStream out) {
        OutputStreams.write((byte) '\\', out);
        OutputStreams.write((byte) 'u', out);
        if (ch > 0xFF) {
            int hi = (ch >> 8) & 0xFF;
            OutputStreams.write((byte) HEX[hi >> 4], out);
            OutputStreams.write((byte) HEX[hi & 0xF], out);
            ch &= 0xFF;
        } else {
            OutputStreams.write((byte) '0', out);
            OutputStreams.write((byte) '0', out);
        }
        OutputStreams.write((byte) HEX[ch >> 4], out);
        OutputStreams.write((byte) HEX[ch & 0xF], out);
    }

    private void writeMultiByteChar(int ch, OutputStream out) {
        if (ch >= SURR1_FIRST && ch <= SURR2_LAST) {
            OutputStreams.write((byte) '\\', out);
            OutputStreams.write((byte) 'u', out);
            OutputStreams.write((byte) HEX[(ch >> 12) & 0xF], out);
            OutputStreams.write((byte) HEX[(ch >> 8) & 0xF], out);
            OutputStreams.write((byte) HEX[(ch >> 4) & 0xF], out);
            OutputStreams.write((byte) HEX[ch & 0xF], out);
        } else {
            OutputStreams.write((byte) (0xe0 | (ch >> 12)), out);
            OutputStreams.write((byte) (0x80 | ((ch >> 6) & 0x3f)), out);
            OutputStreams.write((byte) (0x80 | (ch & 0x3f)), out);
        }
    }
}
