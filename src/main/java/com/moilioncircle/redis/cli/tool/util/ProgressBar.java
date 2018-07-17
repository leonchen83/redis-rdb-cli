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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import static com.moilioncircle.redis.cli.tool.util.Strings.lappend;
import static com.moilioncircle.redis.cli.tool.util.Strings.pretty;

/**
 * @author Baoyi Chen
 */
public class ProgressBar implements Closeable {
    
    private final long ctime;
    private final long total;
    private volatile boolean bit;
    private volatile String file;
    private volatile double percentage;
    private AtomicLong num = new AtomicLong();
    private volatile long atime = System.currentTimeMillis();
    
    public ProgressBar(long total) {
        this.total = total;
        this.ctime = System.currentTimeMillis();
    }
    
    public void react(long num) {
        react(num, true, null);
    }
    
    public void react(long num, String file) {
        react(num, true, file);
    }
    
    public void react(long num, boolean increment, String file) {
        react(num, total <= 0 ? 0 : Processes.width(), increment, file);
    }
    
    public void react(long num, int len, boolean increment, String file) {
        if (increment)
            this.num.addAndGet(num);
        else
            this.num.set(num);
        if (total <= 0) {
            show(-1, -1, len, this.num.get(), file);
            return;
        }
        double percentage = this.num.get() / (double) total * 100;
        int prev = (int) this.percentage;
        this.percentage = percentage;
        int next = (int) this.percentage;
        show(prev, next, len, this.num.get(), file);
    }
    
    private void show(int prev, int next, int len, long num, String file) {
        long now = System.currentTimeMillis();
        long elapsed = now - atime;
        
        if (elapsed < 1000 && prev == next &&
                (file == null || file.equals(this.file))) return;
        int speed = (int) ((double) num / (now - ctime) * 1000);
        this.file = file;
        this.atime = now;
        StringBuilder builder = new StringBuilder();
        if (bit) {
            builder.append('/');
            bit = false;
        } else {
            builder.append('\\');
            bit = true;
        }
        builder.append('[').append(pretty(num));
        if (total <= 0) {
            if (file != null) {
                builder.append('|');
                builder.append(file);
            }
        } else {
            builder.append('/').append(pretty(total)).append('|');
            builder.append(lappend(next, 3, ' ')).append('%');
            if (file != null) {
                builder.append('|');
                builder.append(file);
            }
            builder.append(']');
            int used = builder.length();
            if (len - used < 30) return;
            int ret = len - used - 3;
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
        builder.append('|');
        builder.append(pretty(speed)).append("/s");
        builder.append(']');
        if (total <= 0) builder.append(lappend("", 10, ' '));
        System.out.print('\r');
        System.out.print(builder.toString());
    }
    
    @Override
    public void close() {
        System.out.println();
    }
}
