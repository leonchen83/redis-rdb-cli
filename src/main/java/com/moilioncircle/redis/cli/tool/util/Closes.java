package com.moilioncircle.redis.cli.tool.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * @author Baoyi Chen
 */
public class Closes {

    public static void close(Closeable task) {
        if (task == null) return;
        try {
            task.close();
        } catch (IOException t) {
            throw new UncheckedIOException(t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt);
        }
    }

    public static void closeQuietly(Closeable task) {
        if (task == null) return;
        try {
            task.close();
        } catch (Throwable t) {
        }
    }

}
