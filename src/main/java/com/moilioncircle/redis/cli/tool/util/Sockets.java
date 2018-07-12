package com.moilioncircle.redis.cli.tool.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author Baoyi Chen
 */
public class Sockets {

    public static void close(OutputStream out) {
        if (out == null) return;
        try {
            out.close();
        } catch (IOException t) {
            throw new RuntimeException(t.getMessage(), t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt.getMessage(), txt);
        }
    }

    public static void closeQuietly(OutputStream out) {
        if (out == null) return;
        try {
            out.close();
        } catch (Throwable t) {
        }
    }

    public static void close(InputStream in) {
        if (in == null) return;
        try {
            in.close();
        } catch (IOException t) {
            throw new RuntimeException(t.getMessage(), t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt.getMessage(), txt);
        }
    }

    public static void closeQuietly(InputStream in) {
        if (in == null) return;
        try {
            in.close();
        } catch (Throwable t) {
        }
    }

    public static void close(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException t) {
            throw new RuntimeException(t.getMessage(), t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt.getMessage(), txt);
        }
    }

    public static void closeQuietly(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (Throwable t) {
        }
    }
}
