package com.moilioncircle.redis.cli.tool.util;


import com.moilioncircle.redis.replicator.io.CRCOutputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Integer.parseInt;

/**
 * @author Baoyi Chen
 */
public class NodeConf {
    
    public static void parse(String path, File conf, Set<CRCOutputStream> set, Map<Short, CRCOutputStream> result) {
        Map<String, CRCOutputStream> map = new HashMap<>();
        try (BufferedReader r = new BufferedReader(new FileReader(conf))) {
            String line;
            while ((line = r.readLine()) != null) {
                List<String> args = parseLine(line);
                if (args.isEmpty()) continue;
                if (args.get(0).equals("vars")) {
                    for (int i = 1; i < args.size(); i += 2) {
                        switch (args.get(i)) {
                            case "currentEpoch":
                                // pass
                                break;
                            case "lastVoteEpoch":
                                // pass
                                break;
                            default:
                                break;
                        }
                    }
                } else if (args.size() < 8) {
                    // pass
                } else {
                    String name = args.get(0);
                    if (!map.containsKey(name)) {
                        CRCOutputStream out = OutputStreams.newCRCOutputStream(Paths.get(path, name + ".rdb").toFile());
                        map.put(name, out);
                        set.add(out);
                    }
                    String hostAndPort = args.get(1);
                    int cIdx = hostAndPort.indexOf(":");
                    int aIdx = hostAndPort.indexOf("@");
                    hostAndPort.substring(0, cIdx); // ip
                    parseInt(hostAndPort.substring(aIdx + 1)); // port
                    
                    for (String role : args.get(2).split(",")) {
                        switch (role) {
                            case "noflags":
                                break;
                            case "fail":
                                // pass
                                break;
                            case "fail?":
                                // pass
                                break;
                            case "slave":
                                // pass
                                break;
                            case "noaddr":
                                // pass
                                break;
                            case "master":
                                // pass
                                break;
                            case "handshake":
                                // pass
                                break;
                            case "myself":
                                // pass
                                // pass
                                break;
                            default:
                                // pass
                        }
                    }
                    
                    if (!args.get(3).equals("-")) {
                        args.get(3); // slave
                        // pass
                    }
                    
                    // args.get(4); pingTime
                    // args.get(5); pongTime
                    // args.get(6); configEpoch
                    
                    for (int i = 8; i < args.size(); i++) {
                        int st, ed;
                        String arg = args.get(i);
                        if (arg.startsWith("[")) {
                            int idx = arg.indexOf("-");
                            char direction = arg.charAt(idx + 1);
                            arg.substring(1, idx); // slot
                            arg.substring(idx + 3, idx + 3 + 40); // migrate
                            if (direction == '>') {
                                // pass
                            } else {
                                // pass
                            }
                            continue;
                        } else if (arg.contains("-")) {
                            int idx = arg.indexOf("-");
                            st = parseInt(arg.substring(0, idx));
                            ed = parseInt(arg.substring(idx + 1));
                        } else st = ed = parseInt(arg);
                        for (; st <= ed; st++) {
                            result.put((short) st, map.get(name));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    public static List<String> parseLine(String line) {
        List<String> args = new ArrayList<>();
        if (line.length() == 0 || line.equals("\n")) return args;
        char[] ary = line.toCharArray();
        StringBuilder s = new StringBuilder();
        boolean dq = false, q = false;
        for (int i = 0; i < ary.length; i++) {
            char c = ary[i];
            switch (c) {
                case ' ':
                    if (dq || q) s.append(' ');
                    else if (s.length() > 0) {
                        args.add(s.toString());
                        s.setLength(0);
                    }
                    break;
                case '"':
                    if (!dq && !q) {
                        dq = true;
                    } else if (q) {
                        s.append('"');
                    } else {
                        args.add(s.toString());
                        s.setLength(0);
                        dq = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse config error.");
                    }
                    break;
                case '\'':
                    if (!dq && !q) {
                        q = true;
                    } else if (dq) {
                        s.append('\'');
                    } else {
                        args.add(s.toString());
                        s.setLength(0);
                        q = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse config error.");
                    }
                    break;
                case '\\':
                    if (!dq) s.append('\\');
                    else {
                        i++;
                        if (i < ary.length) {
                            switch (ary[i]) {
                                case 'n':
                                    s.append('\n');
                                    break;
                                case 'r':
                                    s.append('\r');
                                    break;
                                case 't':
                                    s.append('\t');
                                    break;
                                case 'b':
                                    s.append('\b');
                                    break;
                                case 'f':
                                    s.append('\f');
                                    break;
                                case 'a':
                                    s.append((byte) 7);
                                    break;
                                case 'x':
                                    if (i + 2 >= ary.length) s.append("\\x");
                                    else {
                                        char high = ary[++i];
                                        char low = ary[++i];
                                        try {
                                            s.append(parseInt(new String(new char[]{high, low}), 16));
                                        } catch (Exception e) {
                                            s.append("\\x");
                                            s.append(high);
                                            s.append(low);
                                        }
                                    }
                                    break;
                                default:
                                    s.append(ary[i]);
                                    break;
                            }
                        }
                    }
                    break;
                default:
                    s.append(c);
                    break;
            }
        }
        if (dq || q) throw new UnsupportedOperationException("parse line[" + line + "] error.");
        if (s.length() > 0) args.add(s.toString());
        return args;
    }
}
