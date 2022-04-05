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
		List<XStandaloneRedisInfo> infos = new ArrayList<>();
		for (Map.Entry<HostAndPort, XMonitorStandalone> entry : commands.entrySet()) {
			Tuple3<XClusterNodes, XClusterInfo, XStandaloneRedisInfo> tuple = entry.getValue().execute();
			clusterNodes.add(tuple.getV1());
			clusterInfos.add(tuple.getV2());
			infos.add(tuple.getV3());
		}
		XClusterRedisInfo next = XClusterRedisInfo.valueOf(infos, clusterNodes, clusterInfos);
		next = XClusterRedisInfo.diff(prev, next);
		
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
		
		// server
		setLong("cluster_uptime_in_seconds", name, next.getMaster().getUptimeInSeconds());
		setString("cluster_redis_version", name, next.getMaster().getRedisVersion());
		setLong("cluster_maxclients", name, next.getMaster().getMaxclients());
		setLong("cluster_total_system_memory", name, next.getMaster().getTotalSystemMemory());
		
		for (XClusterNode node : next.getClusterNodes().getNodes()) {
			if (node.isMaster()) {
				String[] properties = new String[]{node.getHostAndPort().toString(), name, "master"};
				setLong("cluster_node_slot", properties, (long) node.getSlots().size());
				setLong("cluster_node_migrating_slot", properties, (long) node.getMigratingSlots().size());
			}
		}
		
		for (Map.Entry<String, XStandaloneRedisInfo> entry : next.getMasters().entrySet()) {
			String ip = entry.getKey();
			XStandaloneRedisInfo value = entry.getValue();
			setMonitor(value, ip, name, "master");
		}
		
		for (Map.Entry<String, XStandaloneRedisInfo> entry : next.getSlaves().entrySet()) {
			String ip = entry.getKey();
			XStandaloneRedisInfo value = entry.getValue();
			setMonitor(value, ip, name, "slave");
		}
		
		prev = next;
	}
	
	private void setMonitor(XStandaloneRedisInfo value, String ip, String name, String role) {
		String[] properties = new String[]{ip, name, role};
		setLong("cluster_nodes", properties, (long)1);
		setLong("cluster_connected_clients", properties, value.getConnectedClients());
		setLong("cluster_blocked_clients", properties, value.getBlockedClients());
		setLong("cluster_tracking_clients", properties, value.getTrackingClients());
		setLong("cluster_maxmemory", properties, value.getMaxmemory());
		setLong("cluster_used_memory", properties, value.getUsedMemory());
		setLong("cluster_mem_fragmentation_bytes", properties, value.getMemFragmentationBytes());
		setDouble("cluster_mem_fragmentation_ratio", properties, value.getMemFragmentationRatio());
		setLong("cluster_total_connections_received", properties, value.getTotalConnectionsReceived());
		setLong("cluster_total_commands_processed", properties, value.getTotalCommandsProcessed());
		setLong("cluster_total_reads_processed", properties, value.getTotalReadsProcessed());
		setLong("cluster_total_writes_processed", properties, value.getTotalWritesProcessed());
		setLong("cluster_total_error_replies", properties, value.getTotalErrorReplies());
		Long hits = value.getKeyspaceHits();
		Long misses = value.getKeyspaceMisses();
		if (hits != null && misses != null) {
			monitor.set("cluster_keyspace_hit_rate", properties, hits * 1d / (hits + misses));
		}
		
		setDouble("cluster_used_cpu_sys", properties, value.getUsedCpuSys());
		setDouble("cluster_used_cpu_user", properties, value.getUsedCpuUser());
		setDouble("cluster_used_cpu_sys_children", properties, value.getUsedCpuSysChildren());
		setDouble("cluster_used_cpu_user_children", properties, value.getUsedCpuUserChildren());
		setLong("cluster_dbnum", properties, value.getDbInfo().get("db0"));
		setLong("cluster_dbexp", properties, value.getDbExpireInfo().get("db0"));
		setLong("cluster_total_net_input_bytes", properties, value.getTotalNetInputBytes());
		setLong("cluster_total_net_output_bytes", properties, value.getTotalNetOutputBytes());
		setLong("cluster_expired_keys", properties, value.getExpiredKeys());
		setLong("cluster_evicted_keys", properties, value.getEvictedKeys());
		setLong("cluster_total_slow_log", properties, value.getTotalSlowLog());
		
		if (value.getDiffTotalSlowLog() > 0) {
			monitor.set("cluster_slow_log_latency", properties, (value.getDiffTotalSlowLogExecutionTime() / (value.getDiffTotalSlowLog() * 1d)));
		} else {
			monitor.set("cluster_slow_log_latency", properties, 0d);
		}
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
