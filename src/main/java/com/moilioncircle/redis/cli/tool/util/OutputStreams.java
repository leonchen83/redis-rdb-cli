package com.moilioncircle.redis.cli.tool.util;


import com.moilioncircle.redis.replicator.io.CRCOutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;

/**
 * @author Baoyi Chen
 */
public class OutputStreams {
    
    public static void close(OutputStream out) {
        if (out == null) return;
        try {
            out.close();
        } catch (IOException t) {
            throw new UncheckedIOException(t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt);
        }
    }
    
    public static void closeQuietly(OutputStream out) {
        if (out == null) return;
        try {
            out.close();
        } catch (Throwable t) {
        }
    }
    
    public static void write(byte b, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (IOException t) {
            throw new UncheckedIOException(t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt);
        }
    }
    
    public static void writeQuietly(byte b, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (Throwable t) {
        }
    }
    
    public static void write(byte b[], OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (IOException t) {
            throw new UncheckedIOException(t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt);
        }
    }
    
    public static void writeQuietly(byte b[], OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (Throwable t) {
        }
    }
    
    public static void write(byte b[], int off, int len, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b, off, len);
        } catch (IOException t) {
            throw new UncheckedIOException(t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt);
        }
    }
    
    public static void writeQuietly(byte b[], int off, int len, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b, off, len);
        } catch (Throwable t) {
        }
    }
    
    public static <T extends OutputStream> T call(Callable<T> callable) {
        if (callable == null) return null;
        try {
            return callable.call();
        } catch (IOException t) {
            throw new UncheckedIOException(t);
        } catch (Throwable txt) {
            throw new RuntimeException(txt);
        }
    }
    
    public static <T extends OutputStream> T callQuietly(Callable<T> callable) {
        if (callable == null) return null;
        try {
            return callable.call();
        } catch (Throwable txt) {
            return null;
        }
    }
    
    public static BufferedOutputStream newBufferedOutputStream(String file) {
        return call(() -> newBufferedOutputStream(new File(file)));
    }
    
    public static BufferedOutputStream newBufferedOutputStream(File file) {
        return call(() -> new BufferedOutputStream(new FileOutputStream(file)));
    }
    
    public static CRCOutputStream newCRCOutputStream(String file) {
        return call(() -> newCRCOutputStream(new File(file)));
    }
    
    public static CRCOutputStream newCRCOutputStream(File file) {
        return call(() -> new CRCOutputStream(new BufferedOutputStream(new FileOutputStream(file))));
    }
}
