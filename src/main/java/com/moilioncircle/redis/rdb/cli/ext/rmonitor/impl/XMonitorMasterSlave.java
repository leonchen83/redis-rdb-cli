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
import static com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XStandaloneRedisInfo.extract;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class XMonitorMasterSlave implements MonitorCommand, StandaloneListener {
	
	private static final Logger logger = LoggerFactory.getLogger(XMonitorMasterSlave.class);
	
	private volatile String host;
	private volatile int port;
	private String name;
	private Monitor monitor;
	private Configuration configuration;
	private Map<HostAndPort, XMonitorStandalone> commands = new ConcurrentHashMap<>();
	
	public XMonitorMasterSlave(RedisURI uri, String name, Monitor monitor, Configure configure) {
		this.name = name;
		this.host = uri.getHost();
		this.port = uri.getPort();
		this.monitor = monitor;
		this.configuration = configure.merge(uri, true);
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
				XMonitorStandalone master = new XMonitorStandalone(host, port, name, monitor, configuration);
				master.addListener(this);
				commands.put(new HostAndPort(host, port), master);
				
				// slave
				int slaves = Integer.parseInt(map.get("connected_slaves"));
				for (int i = 0; i < slaves; i++) {
					String[] info = map.get("slave" + i).split(",");
					String slaveHost = info[0].split("=")[1];
					int slavePort = Integer.parseInt(info[1].split("=")[1]);
					XMonitorStandalone slave = new XMonitorStandalone(slaveHost, slavePort, name, monitor, configuration);
					slave.addListener(this);
					commands.put(new HostAndPort(slaveHost, slavePort), slave);
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
	
	@Override
	public void close() throws IOException {
		for (Map.Entry<HostAndPort, XMonitorStandalone> entry : commands.entrySet()) {
			MonitorCommand.closeQuietly(entry.getValue());
		}
	}
	
	@Override
	public void run() {
		for (Map.Entry<HostAndPort, XMonitorStandalone> entry : commands.entrySet()) {
			entry.getValue().run();
		}
	}
	
	@Override
	public void onUp(HostAndPort host) {
		// add
		if (commands.containsKey(host)) {
			return;
		}
		logger.info("master-slave add monitor host [{}]", host);
		XMonitorStandalone slave = new XMonitorStandalone(host.getHost(), host.getPort(), name, monitor, configuration);
		slave.addListener(this);
		XMonitorStandalone prev = commands.put(host, slave);
		if (prev != null) {
			prev.delListener(this);
			MonitorCommand.closeQuietly(prev);
		}
	}
	
	@Override
	public void onDown(HostAndPort host) {
		logger.info("master-slave del monitor host [{}]", host);
		XMonitorStandalone prev = commands.remove(host);
		if (prev != null) {
			prev.delListener(this);
			MonitorCommand.closeQuietly(prev);
		}
	}
}
