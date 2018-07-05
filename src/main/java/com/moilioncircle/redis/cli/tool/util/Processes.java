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

    private static volatile long LAST_ACCESS;

    public static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }

    public static boolean isMac() {
        return (OS.indexOf("mac") >= 0);
    }

    public static boolean isUnix() {
        return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0);
    }

    public static boolean isSolaris() {
        return (OS.indexOf("sunos") >= 0);
    }

    public static int width() {
        long ms = System.currentTimeMillis();
        if (LAST_ACCESS != 0 && ms - LAST_ACCESS < 5000) return WIDTH;
        LAST_ACCESS = ms;
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
