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

package com.moilioncircle.redis.cli.tool.io;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.moilioncircle.redis.cli.tool.util.CRC16.crc16;
import static java.lang.Integer.parseInt;

/**
 * @author Baoyi Chen
 */
public class ShardableFileOutputStream extends OutputStream {
    
    private byte[] key;
    
    private final Set<CRCOutputStream> set = new HashSet<>();
    private final Map<Short, CRCOutputStream> map = new HashMap<>();

    public ShardableFileOutputStream(String path, File conf, Configure configure) {
        NodeConfParser.parse(path, conf, set, map, configure);
        if (map.size() != 16384)
            throw new UnsupportedOperationException("slots size : " + map.size() + ", expected 16384.");
    }
    
    public void shard(byte[] key) {
        this.key = key;
    }
    
    private short slot(byte[] key) {
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
            return (short) (crc16(key, st + 1, ed) & 16383);
        return (short) (crc16(key) & 16383);
    }
    
    @Override
    public void write(int b) throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.write(b);
            }
        } else {
            map.get(slot(key)).write(b);
        }
    }
    
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.write(b, off, len);
            }
        } else {
            map.get(slot(key)).write(b, off, len);
        }
    }
    
    public void flush() throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.flush();
            }
        } else {
            map.get(slot(key)).flush();
        }
    }
    
    public void close() throws IOException {
        for (OutputStream out : set) {
            out.close();
        }
    }
    
    public void writeCRC() {
        for (CRCOutputStream out : set) {
            OutputStreams.write(0xFF, out);
            OutputStreams.write(out.getCRC64(), out);
        }
    }

    private static class NodeConfParser {

        public static void parse(String path, File conf, Set<CRCOutputStream> set, Map<Short, CRCOutputStream> result, Configure configure) {
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
                        String hostAndPort = args.get(1);
                        int cIdx = hostAndPort.indexOf(":");
                        int aIdx = hostAndPort.indexOf("@");
                        hostAndPort.substring(0, cIdx); // ip
                        parseInt(hostAndPort.substring(aIdx + 1)); // port

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
                            CRCOutputStream out = OutputStreams.newCRCOutputStream(Paths.get(path, name + ".rdb").toFile(), configure.getBufferSize());
                            map.put(name, out);
                            set.add(out);
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
                                throw new UnsupportedOperationException(conf.getName() + " must not contains migrating slot.");
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
                throw new UnsupportedOperationException(e.getMessage(), e);
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
}
