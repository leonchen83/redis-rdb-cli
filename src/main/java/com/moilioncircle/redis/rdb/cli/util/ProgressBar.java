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

package com.moilioncircle.redis.rdb.cli.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Baoyi Chen
 */
public abstract class ProgressBar implements Closeable {
    
    public static ProgressBar bar(boolean enable) throws IOException {
        return enable ? new EnableBar() : new DisableBar();
    }

    public abstract void react(long num);

    public abstract void react(long num, String file);

    public abstract void react(long num, boolean increment, String file);

    private static class DisableBar extends ProgressBar {
    
        @Override
        public void react(long num) {
        }
    
        @Override
        public void react(long num, String file) {
        }
    
        @Override
        public void react(long num, boolean increment, String file) {
        }
    
        @Override
        public void close() throws IOException {
        }
    }
    
    private static class EnableBar extends ProgressBar {
        private final long ctime;
        private volatile int max = 0;
        private volatile boolean bit;
        private volatile String file;
        private volatile boolean first = true;
        private AtomicLong num = new AtomicLong();
        private volatile long atime = System.currentTimeMillis();
    
        public EnableBar() throws IOException {
            this.ctime = System.currentTimeMillis();
        }
    
        @Override
        public void react(long num) {
            react(num, true, null);
        }
    
        @Override
        public void react(long num, String file) {
            react(num, true, file);
        }
    
        @Override
        public void react(long num, boolean increment, String file) {
            if (increment)
                this.num.addAndGet(num);
            else
                this.num.set(num);
            show(-1, -1, this.num.get(), file);
        }
    
        private void show(int prev, int next, long num, String file) {
            long now = System.currentTimeMillis();
            long elapsed = now - atime;
        
            if (first) {
                first = false;
            } else if (elapsed < 1000 && prev == next && (file == null || file.equals(this.file))) {
                return;
            }
        
            // avoid divide 0
            elapsed = Math.max(now - ctime, 1);
            int rawSpeed = (int) ((double) num / elapsed * 1000);
            String speed = Strings.lappend(Strings.pretty(rawSpeed), 7, ' ');
            this.file = file;
            this.atime = now;
            this.max = Math.max(Strings.length(file), max);
        
            StringBuilder builder = new StringBuilder();
            if (bit) {
                builder.append('/');
                bit = false;
            } else {
                builder.append('\\');
                bit = true;
            }
            builder.append('[').append(Strings.lappend(Strings.pretty(num), 7, ' '));
            if (file != null) {
                builder.append('|');
                builder.append(Strings.lappend(file, max, ' '));
            }
            builder.append('|');
            builder.append(speed).append("/s");
            builder.append(']');
            System.out.print('\r');
            System.out.print(builder.toString());
        }
    
        @Override
        public void close() {
            System.out.println();
        }
    }
}
