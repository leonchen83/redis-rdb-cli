package com.moilioncircle.redis.rdb.cli.conf;

import com.moilioncircle.redis.rdb.cli.util.CRC16;
import com.moilioncircle.redis.rdb.cli.util.Tuples;
import com.moilioncircle.redis.rdb.cli.util.type.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.lang.Integer.parseInt;

/**
 * @author Baoyi Chen
 */
public class NodeConfParser<T> {

    private final Function<Tuple3<String, Integer, String>, T> mapper;

    public NodeConfParser(Function<Tuple3<String, Integer, String>, T> mapper) {
        this.mapper = mapper;
    }

    public void parse(List<String> conf, Set<T> set, Map<Short, T> result) {
        Map<String, T> map = new HashMap<>();
        for (String line : conf) {
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
                String hostAndPort = args.get(1);
                int cIdx = hostAndPort.indexOf(":");
                int aIdx = hostAndPort.indexOf("@");
                String host = hostAndPort.substring(0, cIdx); // ip
                int port = parseInt(hostAndPort.substring(cIdx + 1, aIdx == -1 ? hostAndPort.length() : aIdx));

                boolean master = false;
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
                            master = true;
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

                if (!map.containsKey(name) && master) {
                    T v = mapper.apply(Tuples.of(host, port, name));
                    map.put(name, v);
                    set.add(v);
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
                        arg.substring(1, idx); // slot
                        arg.substring(idx + 3, idx + 3 + 40); // migrate
                        throw new UnsupportedOperationException("must not contains migrating slot.");
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

        if (result.size() != 16384)
            throw new UnsupportedOperationException("slots size : " + map.size() + ", expected 16384.");
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

    public static short slot(byte[] key) {
        if (key == null) return 0;
        int st = -1, ed = -1;
        for (int i = 0, len = key.length; i < len; i++) {
            if (key[i] == '{' && st == -1) st = i;
            if (key[i] == '}' && st >= 0) {
                ed = i;
                break;
            }
        }
        if (st >= 0 && ed >= 0 && ed > st + 1)
            return (short) (CRC16.crc16(key, st + 1, ed) & 16383);
        return (short) (CRC16.crc16(key) & 16383);
    }
}