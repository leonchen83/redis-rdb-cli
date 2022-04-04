/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli.ext.rmonitor.impl;

import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.INFO;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.REPLICATION;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.MonitorCommand;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.util.Strings;

import redis.clients.jedis.HostAndPort;

/**
 * @author Baoyi Chen
 */
public class XMonitorMasterSlave implements MonitorCommand {
	
	private volatile String host;
	private volatile int port;
	private String name;
	private Monitor monitor;
	private Map<HostAndPort, MonitorCommand> commands = new ConcurrentHashMap<>();
	
	public XMonitorMasterSlave(RedisURI uri, String name, Monitor monitor, Configure configure) {
		this.name = name;
		this.host = uri.getHost();
		this.port = uri.getPort();
		this.monitor = monitor;
		Configuration configuration = configure.merge(uri, true);
		createMonitorCommands(host, port, configuration);
	}
	
	protected void createMonitorCommands(String host, int port, Configuration configuration) {
		try(XEndpoint endpoint = new XEndpoint(host, port, 0, -1, false, configuration)) {
			String replication = endpoint.send(INFO, REPLICATION).getString();
			Map<String, String> map = extract(replication).get("Replication");
			if (map == null || map.isEmpty()) {
				throw new UnsupportedOperationException();
			}
			String role = map.get("role");
			if (Strings.isEquals(role, "master")) {
				// master
				commands.put(new HostAndPort(host, port), new XMonitorStandalone(host, port, name, monitor, configuration));
				
				// slave
				int slaves = Integer.parseInt(map.get("connected_slaves"));
				for (int i = 0; i < slaves; i++) {
					String[] slave = map.get("slave" + i).split(",");
					String slaveHost = slave[0].split("=")[1];
					int slavePort = Integer.parseInt(slave[1].split("=")[1]);
					commands.put(new HostAndPort(slaveHost, slavePort), new XMonitorStandalone(slaveHost, slavePort, name, monitor, configuration));
				}
			} else {
				this.host = map.get("master_host");
				this.port = Integer.parseInt(map.get("master_port"));
				createMonitorCommands(this.host, this.port, configuration);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Map<String, Map<String, String>> extract(String info) {
		Map<String, Map<String, String>> map = new HashMap<>(16);
		String[] lines = info.split("\n");
		Map<String, String> value = null;
		for (String line : lines) {
			line = line == null ? "" : line.trim();
			if (line.startsWith("#")) {
				String key = line.substring(1).trim();
				value = new HashMap<>(128);
				map.put(key, value);
			} else if (line.length() != 0) {
				String[] ary = line.split(":");
				if (ary.length == 2) {
					if (value != null) value.put(ary[0], ary[1]);
				}
			}
		}
		return map;
	}
	
	@Override
	public void close() throws IOException {
		for (Map.Entry<HostAndPort, MonitorCommand> entry : commands.entrySet()) {
			MonitorCommand.closeQuietly(entry.getValue());
		}
	}
	
	@Override
	public void run() {
		for (Map.Entry<HostAndPort, MonitorCommand> entry : commands.entrySet()) {
			entry.getValue().run();
		}
	}
}
