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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;

import com.moilioncircle.redis.replicator.io.CRCOutputStream;

/**
 * @author Baoyi Chen
 */
public class OutputStreams {
    
    public static void close(OutputStream out) {
        if (out == null) return;
        try {
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void closeQuietly(OutputStream out) {
        if (out == null) return;
        try {
            out.close();
        } catch (Throwable t) {
        }
    }
    
    public static void write(int b, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void writeQuietly(int b, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (Throwable t) {
        }
    }
    
    public static void write(byte[] b, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void writeQuietly(byte b[], OutputStream out) {
        if (out == null) return;
        try {
            out.write(b);
        } catch (Throwable t) {
        }
    }
    
    public static void write(byte[] b, int off, int len, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b, off, len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void writeQuietly(byte[] b, int off, int len, OutputStream out) {
        if (out == null) return;
        try {
            out.write(b, off, len);
        } catch (Throwable t) {
        }
    }
    
    public static void flushQuietly(OutputStream out) {
        if (out == null) return;
        try {
            out.flush();
        } catch (Throwable t) {
        }
    }
    
    public static void flush(OutputStream out) {
        if (out == null) return;
        try {
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static <T> T call(Callable<T> callable) {
        if (callable == null) return null;
        try {
            return callable.call();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static <T> T callQuietly(Callable<T> callable) {
        if (callable == null) return null;
        try {
            return callable.call();
        } catch (Throwable txt) {
            return null;
        }
    }
    
    public static BufferedOutputStream newBufferedOutputStream(String file, int buf) {
        return call(() -> newBufferedOutputStream(new File(file), buf));
    }
    
    public static BufferedOutputStream newBufferedOutputStream(File file, int buf) {
        return call(() -> new BufferedOutputStream(new FileOutputStream(file), buf));
    }
    
    public static CRCOutputStream newCRCOutputStream(String file, int buf) {
        return call(() -> newCRCOutputStream(new File(file), buf));
    }
    
    public static CRCOutputStream newCRCOutputStream(File file, int buf) {
        return call(() -> new CRCOutputStream(new BufferedOutputStream(new FileOutputStream(file), buf)));
    }
}
