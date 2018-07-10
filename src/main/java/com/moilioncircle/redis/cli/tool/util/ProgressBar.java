package com.moilioncircle.redis.cli.tool.util;

import com.moilioncircle.redis.cli.tool.glossary.Phase;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static com.moilioncircle.redis.cli.tool.util.Strings.pretty;

/**
 * @author Baoyi Chen
 */
public class ProgressBar implements Closeable {

    private final long total;
    private volatile long last;
    private volatile boolean bit;
    private volatile double percentage;
    private AtomicLong num = new AtomicLong();
    private volatile long access = System.currentTimeMillis();

    public ProgressBar(long total) {
        this.total = total;
    }

    public void react(long num) {
        react(num, true, null, null);
    }

    public void react(long num, Phase phase) {
        react(num, true, phase, null);
    }

    public void react(long num, Phase phase, File file) {
        react(num, true, phase, file);
    }

    public void react(long num, boolean increment, Phase phase, File file) {
        react(num, total <= 0 ? 0 : Processes.width(), increment, phase, file);
    }

    public void react(long num, int len, boolean increment, Phase phase, File file) {
        if (increment)
            this.num.addAndGet(num);
        else
            this.num.set(num);
        if (total <= 0) {
            show(0, 0, len, this.num.get(), phase, file);
            return;
        }
        double percentage = this.num.get() / (double) total * 100;
        if (this.percentage != percentage) {
            double prev = this.percentage;
            this.percentage = percentage;
            double next = this.percentage;
            show(prev, next, len, this.num.get(), phase, file);
        }
    }

    private void show(double prev, double next, int len, long num, Phase phase, File file) {
        long now = System.currentTimeMillis();
        long elapsed = now - access;
        if (elapsed < 1000) return;
        int speed = (int) ((double) (num - last) / elapsed * 1000);
        last = num;
        access = now;
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
            if (phase != null) {
                builder.append('|');
                builder.append(phase);
            }
            if (file != null) {
                builder.append('|');
                builder.append(file.getName());
            }
        } else {
            builder.append('/').append(pretty(total)).append('|');
            if ((int) next < 10) {
                builder.append(' ').append(' ').append((int) next).append('%');
            } else if ((int) next < 100) {
                builder.append(' ').append((int) next).append('%');
            } else {
                builder.append((int) next).append('%');
            }
            if (phase != null) {
                builder.append('|');
                builder.append(phase);
            }
            if (file != null) {
                builder.append('|');
                builder.append(file.getName());
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
        System.out.print('\r');
        System.out.print(builder.toString());
    }

    @Override
    public void close() {
        System.out.println();
    }
}
