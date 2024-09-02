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

package com.moilioncircle.redis.rdb.cli.conf;

import static com.moilioncircle.redis.rdb.cli.util.Collections.isEmpty;
import static java.lang.Integer.parseInt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.moilioncircle.redis.rdb.cli.glossary.Slotable;
import com.moilioncircle.redis.rdb.cli.util.CRC16;
import com.moilioncircle.redis.rdb.cli.util.Collections;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple3;

import redis.clients.jedis.HostAndPort;

/**
 * @author Baoyi Chen
 */
public class NodeConfParser {
	
	public static XClusterNodes parse(String clusterNodes) {
		return parse(Collections.ofList(clusterNodes.split("\n")), Collections.ofSet(), new HashMap<>(), null);
	}
	
	public static <T> XClusterNodes parse(List<String> conf, Set<T> set, Map<Short, T> result, Function<Tuple3<String, Integer, String>, T> mapper) {
		XClusterNodes nodes = new XClusterNodes();
		List<XClusterNode> list = new ArrayList<>(conf.size());
		nodes.setNodes(list);
		Map<String, T> map = new HashMap<>();
		for (String line : conf) {
			List<String> args = parseLine(line);
			if (isEmpty(args)) continue;
			if (args.get(0).equals("vars")) {
				for (int i = 1; i < args.size(); i += 2) {
					switch (args.get(i)) {
						case "currentEpoch":
							nodes.setCurrentEpoch(Long.parseLong(args.get(i + 1)));
							break;
						case "lastVoteEpoch":
							nodes.setLastVoteEpoch(Long.parseLong(args.get(i + 1)));
							break;
						default:
							break;
					}
				}
			} else if (args.size() < 8) {
				// pass
			} else {
				XClusterNode node = new XClusterNode();
				list.add(node);
				String name = args.get(0);
				node.setName(name);
				String hostAndPort = args.get(1);
				int cIdx = hostAndPort.indexOf(":");
				int aIdx = hostAndPort.indexOf("@");
				int commaIdx = hostAndPort.indexOf(",");
				String host = "";
				if (commaIdx == -1) {
					host = hostAndPort.substring(0, cIdx);
				} else {
					host = hostAndPort.substring(commaIdx + 1);
				}
				int port = parseInt(hostAndPort.substring(cIdx + 1, aIdx == -1 ? hostAndPort.length() : aIdx));
				node.setHostAndPort(new HostAndPort(host, port));
				boolean master = false;
				boolean serving = true;
				for (String role : args.get(2).split(",")) {
					switch (role) {
						case "fail":
						case "fail?":
							node.setState(role);
							serving = false;
							break;
						case "noflags":
						case "noaddr":
						case "handshake":
							serving = false;
							break;
						case "slave":
							node.setMaster(false);
							serving = false;
							break;
						case "master":
							node.setMaster(true);
							master = true;
							break;
						case "myself":
							node.setMyself(true);
							break;
						default:
							serving = false;
					}
				}
				
				if (!map.containsKey(name) && master && serving) {
					if (mapper != null) {
						T v = mapper.apply(Tuples.of(host, port, name));
						map.put(name, v);
						set.add(v);
					}
				}
				
				if (!args.get(3).equals("-")) {
					args.get(3); // slave
					// pass
				}
				
				String pingTime = args.get(4);
				node.setPingTime(Long.parseLong(pingTime));
				String pongTime = args.get(5);
				node.setPongTime(Long.parseLong(pongTime));
				String configEpoch = args.get(6);
				node.setConfigEpoch(Long.parseLong(configEpoch));
				String connect = args.get(7);
				node.setLink(connect);
				
				for (int i = 8; i < args.size(); i++) {
					int st = 0, ed = 0;
					String arg = args.get(i);
					if (arg.startsWith("[")) {
						int idx = arg.indexOf("-");
						String slot = arg.substring(1, idx); // slot
						arg.substring(idx + 3, idx + 3 + 40); // migrate
						node.getMigratingSlots().add(Short.parseShort(slot));
					} else if (arg.contains("-")) {
						int idx = arg.indexOf("-");
						st = parseInt(arg.substring(0, idx));
						ed = parseInt(arg.substring(idx + 1));
					} else {
						st = ed = parseInt(arg);
					}
					for (; st <= ed; st++) {
						T v = map.get(name);
						result.put((short) st, v);
						node.getSlots().add((short) st);
						if (v instanceof Slotable) {
							((Slotable) v).addSlot((short) st);
						}
					}
				}
			}
		}
		
		return nodes;
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