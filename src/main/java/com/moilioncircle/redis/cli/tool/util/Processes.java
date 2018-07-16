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

import com.moilioncircle.redis.replicator.util.Strings;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Baoyi Chen
 */
public class Processes {
    
    private static String OS = System.getProperty("os.name").toLowerCase();
    
    private static volatile int WIDTH = 120;

    private static volatile long ATIME;
    
    public static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }
    
    public static boolean isMac() {
        return (OS.indexOf("mac") >= 0);
    }

    public static boolean isSolaris() {
        return (OS.indexOf("sunos") >= 0);
    }
    
    public static boolean isUnix() {
        return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0);
    }
    
    public static int width() {
        long ms = System.currentTimeMillis();
        if (ATIME != 0 && ms - ATIME < 5000) return WIDTH;
        ATIME = ms;
        if (isUnix()) {
            try {
                DefaultExecutor exec = new DefaultExecutor();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                exec.setStreamHandler(new PumpStreamHandler(out));
                int r = exec.execute(CommandLine.parse("tput cols"));
                if (r == 0) WIDTH = Integer.parseInt(Strings.toString(out.toByteArray()));
            } catch (IOException e) {
            }
        }
        return WIDTH;
    }
}
