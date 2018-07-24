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

package com.moilioncircle.redis.cli.tool.util;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.moilioncircle.redis.cli.tool.util.Strings.lappend;
import static com.moilioncircle.redis.cli.tool.util.Strings.length;
import static com.moilioncircle.redis.cli.tool.util.Strings.pretty;

/**
 * @author Baoyi Chen
 */
public class ProgressBar implements Closeable {

    private final long ctime;
    private final long total;
    private volatile int max = 0;
    private volatile boolean bit;
    private volatile String file;
    private final Terminal terminal;
    private volatile double percentage;
    private AtomicLong num = new AtomicLong();
    private volatile long atime = System.currentTimeMillis();

    public ProgressBar(long total) throws IOException {
        this.total = total;
        this.ctime = System.currentTimeMillis();
        this.terminal = TerminalBuilder.terminal();
    }

    public void react(long num) {
        react(num, true, null);
    }

    public void react(long num, String file) {
        react(num, true, file);
    }

    public void react(long num, boolean increment, String file) {
        if (increment)
            this.num.addAndGet(num);
        else
            this.num.set(num);
        if (total <= 0) {
            show(-1, -1, this.num.get(), file);
            return;
        }
        double percentage = this.num.get() / (double) total * 100;
        int prev = (int) this.percentage;
        this.percentage = percentage;
        int next = (int) this.percentage;
        show(prev, next, this.num.get(), file);
    }

    private void show(int prev, int next, long num, String file) {
        long now = System.currentTimeMillis();
        long elapsed = now - atime;

        if (elapsed < 1000 && prev == next &&
                (file == null || file.equals(this.file))) return;
        int speed = (int) ((double) num / (now - ctime) * 1000);
        String strSpeed = lappend(pretty(speed), 7, ' ');
        this.file = file;
        this.atime = now;
        this.max = Math.max(length(file), max);
        int len = Math.max(terminal.getWidth(), 120);

        StringBuilder builder = new StringBuilder();
        if (bit) {
            builder.append('/');
            bit = false;
        } else {
            builder.append('\\');
            bit = true;
        }
        builder.append('[').append(lappend(pretty(num), 7, ' '));
        if (total <= 0) {
            if (file != null) {
                builder.append('|');
                builder.append(lappend(file, max, ' '));
            }
        } else {
            builder.append('/').append(lappend(pretty(total), 7, ' ')).append('|');
            builder.append(lappend(next, 3, ' ')).append('%');
            if (file != null) {
                builder.append('|');
                builder.append(lappend(file, max, ' '));
            }
            builder.append(']');
            int used = builder.length();
            if (len - used >= 30 + 5 + strSpeed.length()) {
                int ret = len - used - 5 - strSpeed.length();
                int n = (int) (next * (ret / 100d));
                builder.append('[');
                for (int i = 0; i < ret; i++) {
                    if (i < n) {
                        builder.append('#');
                    } else {
                        builder.append('-');
                    }
                }
            }
        }
        builder.append('|');
        builder.append(strSpeed).append("/s");
        builder.append(']');
        System.out.print('\r');
        System.out.print(builder.toString());
    }

    @Override
    public void close() {
        System.out.println();
    }
}
