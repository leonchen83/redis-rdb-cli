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

import static com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XClusterRedisInfo.EMPTY_CLUSTER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.conf.XClusterNode;
import com.moilioncircle.redis.rdb.cli.conf.XClusterNodes;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.MonitorCommand;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XClusterInfo;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XClusterRedisInfo;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XStandaloneRedisInfo;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.util.type.Tuple3;

import redis.clients.jedis.HostAndPort;

/**
 * @author Baoyi Chen
 */
public class XMonitorCluster implements MonitorCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(XMonitorCluster.class);
	
	private String name;
	private Monitor monitor;
	private Configuration configuration;
	private XClusterRedisInfo prev = EMPTY_CLUSTER;
	private Map<HostAndPort, XMonitorStandalone> commands = new ConcurrentHashMap<>();
	
	public XMonitorCluster(String clusterNodes, String name, Monitor monitor, Configuration configuration) {
		this.name = name;
		this.monitor = monitor;
		this.configuration = configuration;
		createMonitorCommands(clusterNodes, configuration);
	}
	
	protected void createMonitorCommands(String clusterNodes, Configuration configuration) {
		XClusterNodes nodes = NodeConfParser.parse(clusterNodes);
		for (XClusterNode node : nodes.getNodes()) {
			HostAndPort hostAndPort = node.getHostAndPort();
			String host = hostAndPort.getHost();
			int port = hostAndPort.getPort();
			XMonitorStandalone value = new XMonitorStandalone(host, port, name, monitor, configuration, true);
			commands.put(hostAndPort, value);
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
		List<XClusterNodes> clusterNodes = new ArrayList<>();
		List<XClusterInfo> clusterInfos = new ArrayList<>();
		for (Map.Entry<HostAndPort, XMonitorStandalone> entry : commands.entrySet()) {
			Tuple3<XClusterNodes, XClusterInfo, XStandaloneRedisInfo> tuple = entry.getValue().execute();
			if (tuple != null) {
				clusterNodes.add(tuple.getV1());
				clusterInfos.add(tuple.getV2());
			}
		}
		XClusterRedisInfo next = XClusterRedisInfo.valueOf(clusterNodes, clusterInfos);
		
		XClusterNodes prevNodes = prev.getClusterNodes();
		XClusterNodes nextNodes = next.getClusterNodes();
		
		if (prevNodes.getCurrentEpoch() != nextNodes.getCurrentEpoch()) {
			List<HostAndPort> prevHosts = prevNodes.getNodes().stream().map(e -> e.getHostAndPort()).collect(Collectors.toList());
			List<HostAndPort> nextHosts = nextNodes.getNodes().stream().map(e -> e.getHostAndPort()).collect(Collectors.toList());
			for (HostAndPort hp : nextHosts) {
				if(!prevHosts.contains(hp)) {
					monitor.set("cluster_redis_status", hp.toString(), name, "ok");
					onUp(hp);
				}
			}
			
			for (HostAndPort hp : prevHosts) {
				if (!nextHosts.contains(hp)) {
					monitor.set("cluster_redis_status", hp.toString(), name, "down");
					onDown(hp);
				}
			}
		}
		
		setLong("cluster_monitor", name, System.currentTimeMillis());
		setString("cluster_state", name, next.getClusterInfo().getClusterState());
		setLong("cluster_slot_assigned", name, next.getClusterInfo().getClusterSlotsAssigned());
		setLong("cluster_slot_ok", name, next.getClusterInfo().getClusterSlotsOk());
		setLong("cluster_slot_fail", name, next.getClusterInfo().getClusterSlotsFail());
		setLong("cluster_slot_pfail", name, next.getClusterInfo().getClusterSlotsPfail());
		setLong("cluster_known_nodes", name, next.getClusterInfo().getClusterKnownNodes());
		setLong("cluster_size", name, next.getClusterInfo().getClusterSize());
		setLong("cluster_current_epoch", name, next.getClusterInfo().getClusterCurrentEpoch());
		setLong("cluster_stats_messages_received", name, next.getClusterInfo().getClusterStatsMessagesReceived());
		setLong("cluster_stats_messages_sent", name, next.getClusterInfo().getClusterStatsMessagesSent());
		
		for (XClusterNode node : next.getClusterNodes().getNodes()) {
			if (node.isMaster()) {
				String[] properties = new String[]{node.getHostAndPort().toString(), name, "master"};
				setLong("cluster_node_slot", properties, (long) node.getSlots().size());
				setLong("cluster_node_migrating_slot", properties, (long) node.getMigratingSlots().size());
			}
		}
		
		prev = next;
	}
	
	private void setLong(String field, String property, Long value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setDouble(String field, String property, Double value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setString(String field, String property, String value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setLong(String field, String[] property, Long value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setDouble(String field, String[] property, Double value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	private void setString(String field, String[] property, String value) {
		if (value != null) {
			monitor.set(field, property, value);
		}
	}
	
	public void onUp(HostAndPort host) {
		// add
		if (commands.containsKey(host)) {
			return;
		}
		logger.info("master-slave add monitor host [{}]", host);
		XMonitorStandalone slave = new XMonitorStandalone(host.getHost(), host.getPort(), name, monitor, configuration);
		XMonitorStandalone prev = commands.put(host, slave);
		if (prev != null) {
			MonitorCommand.closeQuietly(prev);
		}
	}
	
	public void onDown(HostAndPort host) {
		logger.info("master-slave del monitor host [{}]", host);
		XMonitorStandalone prev = commands.remove(host);
		if (prev != null) {
			MonitorCommand.closeQuietly(prev);
		}
	}
}
