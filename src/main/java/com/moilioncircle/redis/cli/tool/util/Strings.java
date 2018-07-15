package com.moilioncircle.redis.cli.tool.util;

/**
 * @author Baoyi Chen
 */
public class Strings {
    public static String pretty(long bytes) {
        return pretty(bytes, true);
    }

    public static String pretty(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + "B";
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
