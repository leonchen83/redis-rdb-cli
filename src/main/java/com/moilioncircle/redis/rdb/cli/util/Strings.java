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

/**
 * @author Baoyi Chen
 */
public class Strings {

    public static int length(String str) {
        return str == null ? 0 : str.length();
    }
    
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
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
    
    public static String lappend(String src, int len, char c) {
        if (src.length() >= len) return src;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len - src.length(); i++) {
            sb.append(c);
        }
        return sb.append(src).toString();
    }
    
    public static String lappend(int src, int len, char c) {
        return lappend(String.valueOf(src), len, c);
    }
}
