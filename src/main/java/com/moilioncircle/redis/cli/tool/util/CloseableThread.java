package com.moilioncircle.redis.cli.tool.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public final class CloseableThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(CloseableThread.class);

    private final Runnable runnable;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public CloseableThread(Runnable runnable) {
        this.runnable = runnable;
    }

    public CloseableThread(String name, Runnable runnable) {
        this.runnable = runnable; this.setName(name);
    }

    public boolean close() {
        boolean r = stopped.compareAndSet(false, true);
        if (r) this.interrupt();
        return r;
    }

    @Override
    public void run() {
        while (!this.stopped.get()) {
            try {
                this.runnable.run();
            } catch (Throwable tx) {
                final String t = getId() + "@" + getName();
                logger.error("failed to run, thread: " + t, tx); break;
            }
        }
    }

    public static final CloseableThread open(final String name, final Runnable runnable) {
        return open(name, runnable, true);
    }

    public static final void close(final CloseableThread thread) {
        if (thread != null) thread.close();
    }

    public static final CloseableThread open(final String name, final Runnable runnable, boolean start) {
        return open(name, runnable, start, false);
    }

    public static final CloseableThread open(final String name, final Runnable runnable, boolean start, boolean daemon) {
        final CloseableThread r = new CloseableThread(name, runnable); r.setDaemon(daemon); if (start) r.start();
        return r;
    }
}
