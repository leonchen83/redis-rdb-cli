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

    public static void main(String[] args) {
        System.out.println('\u0022');
    }
}
