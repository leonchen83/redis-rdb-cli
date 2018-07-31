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
