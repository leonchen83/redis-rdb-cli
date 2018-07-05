package com.moilioncircle.redis.cli.tool.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Baoyi Chen
 */
public class ProgressBar {

    public enum Phase {
        RDB, AOF, NOP
    }

    private final long total;
    private volatile long last;
    private volatile boolean bit;
    private volatile double percentage;
    private AtomicLong num = new AtomicLong();
    private volatile long access = System.currentTimeMillis();

    public ProgressBar(long total) {
        this.total = total;
    }

    public void react(long num, Phase phase) {
        react(num, true, phase);
    }

    public void react(long num, boolean increment, Phase phase) {
        react(num, total < 0 ? 0 : Processes.width(), increment, phase);
    }

    public void react(long num, int len, boolean increment, Phase phase) {
        if (increment)
            this.num.addAndGet(num);
        else
            this.num.set(num);
        double percentage = this.num.get() / (double) total * 100;
        if (this.percentage != percentage) {
            double prev = this.percentage;
            this.percentage = percentage;
            double next = this.percentage;
            show(prev, next, len, this.num.get(), this.total, phase);
        }
    }

    private void show(double prev, double next, int len, long num, long total, Phase phase) {
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
        if (total > 0) {
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
        } else {
            if (phase != null) {
                builder.append('|');
                builder.append(phase);
            }
        }
        builder.append('|');
        builder.append(pretty(speed)).append("/s");
        builder.append(']');
        System.out.print('\r');
        System.out.print(builder.toString());
    }

    public static String pretty(long bytes) {
        return pretty(bytes, true);
    }

    public static String pretty(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f%sB", bytes / Math.pow(unit, exp), pre);
    }
}
