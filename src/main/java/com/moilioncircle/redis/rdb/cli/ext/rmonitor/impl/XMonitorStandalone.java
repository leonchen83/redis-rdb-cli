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


import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.ALL;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.CLUSTER;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.CONFIG;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.GET;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.INFO;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.LEN;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.MAXCLIENTS;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.NODES;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.SLOWLOG;
import static com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XStandaloneRedisInfo.EMPTY;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.conf.XClusterNodes;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.MonitorCommand;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XClusterInfo;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XSlowLog;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XStandaloneRedisInfo;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.util.Strings;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple3;

import redis.clients.jedis.HostAndPort;

/**
 * @author Baoyi Chen
 */
public class XMonitorStandalone implements MonitorCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(XMonitorStandalone.class);
	
	private final String host;
	private final int port;
	private final String name;
	private final boolean cluster;
	private final Monitor monitor;
	private final String hostAndPort;
	
	private volatile XEndpoint endpoint;
	
	private final Configuration configuration;
	private XStandaloneRedisInfo prev = EMPTY;
	private Tuple3<XClusterNodes, XClusterInfo, XStandaloneRedisInfo> info;
	private List<StandaloneListener> listeners = new CopyOnWriteArrayList<>();
	
	public void addListener(StandaloneListener listener) {
		listeners.add(listener);
	}
	
	public void delListener(StandaloneListener listener) {
		listeners.remove(listener);
	}
	
	public void notifyUp(HostAndPort host) {
		for (StandaloneListener listener : listeners) {
			listener.onUp(host);
		}
		monitor.set("redis_status", host.toString(), name, "ok");
	}
	
	public void notifyDown(HostAndPort host) {
		for (StandaloneListener listener : listeners) {
			listener.onDown(host);
		}
		monitor.set("redis_status", host.toString(), name, "down");
	}
	
	public XMonitorStandalone(String host, int port, String name, Monitor monitor, Configuration configuration) {
		this(host, port, name, monitor, configuration, false);
	}
	
	public XMonitorStandalone(String host, int port, String name, Monitor monitor, Configuration configuration, boolean cluster) {
		this.name = name;
		this.host = host;
		this.port = port;
		this.monitor = monitor;
		this.configuration = configuration;
		this.cluster = cluster;
		this.hostAndPort = new HostAndPort(host, port).toString();
	}
	
	public Tuple3<XClusterNodes, XClusterInfo, XStandaloneRedisInfo> execute() {
		run();
		return this.info;
	}
	
	@Override
	public void run() {
		try {
			if (endpoint == null) {
				this.endpoint = new XEndpoint(this.host, this.port, 0, -1, false, configuration);
			}
			
			endpoint.batch(true, INFO, ALL);
			endpoint.batch(true, CONFIG, GET, MAXCLIENTS);
			endpoint.batch(true, SLOWLOG, LEN);
			endpoint.batch(true, SLOWLOG, GET, "128".getBytes());
			if (cluster) {
				endpoint.batch(true, CLUSTER, NODES);
				endpoint.batch(true, CLUSTER, INFO);
			}
			List<RedisObject> list = endpoint.sync();
			
			String info = list.get(0).getString();
			String maxclients = list.get(1).getArray()[1].getString();
			Long len = list.get(2).getNumber();
			RedisObject[] binaryLogs = list.get(3).getArray();
			
			XStandaloneRedisInfo next = XStandaloneRedisInfo.valueOf(info, maxclients, len, binaryLogs, hostAndPort);
			next = XStandaloneRedisInfo.diff(prev, next);
			
			if (cluster) {
				XClusterNodes clusterNodes = NodeConfParser.parse(list.get(4).getString());
				XClusterInfo clusterInfo = XClusterInfo.valueOf(list.get(5).getString());
				clusterNodes.setCurrentEpoch(clusterInfo.getClusterCurrentEpoch());
				this.info = Tuples.of(clusterNodes, clusterInfo, next);
			}
			
			// server
			long now = System.currentTimeMillis();
			String role = next.getRole();
			setLong("monitor", hostAndPort, name, role, now);
			setLong("uptime_in_seconds", hostAndPort, name, role, next.getUptimeInSeconds());
			setString("redis_version", hostAndPort, name, role, next.getRedisVersion());
			monitor.set("role", hostAndPort, name, next.getRole());
			
			// clients
			setLong("connected_clients", hostAndPort, name, role, next.getConnectedClients());
			setLong("blocked_clients", hostAndPort, name, role, next.getBlockedClients());
			setLong("tracking_clients", hostAndPort, name, role, next.getTrackingClients());
			setLong("maxclients", hostAndPort, name, role, next.getMaxclients());
			
			// memory
			setLong("maxmemory", hostAndPort, name, role, next.getMaxmemory());
			setLong("used_memory", hostAndPort, name, role, next.getUsedMemory());
			setLong("used_memory_rss", hostAndPort, name, role, next.getUsedMemoryRss());
			setLong("used_memory_peak", hostAndPort, name, role, next.getUsedMemoryPeak());
			setLong("used_memory_dataset", hostAndPort, name, role, next.getUsedMemoryDataset());
			setLong("used_memory_lua", hostAndPort, name, role, next.getUsedMemoryLua());
			setLong("used_memory_functions", hostAndPort, name, role, next.getUsedMemoryFunctions());
			setLong("used_memory_scripts", hostAndPort, name, role, next.getUsedMemoryScripts());
			setLong("total_system_memory", hostAndPort, name, role, next.getTotalSystemMemory()); // ?
			setDouble("mem_fragmentation_ratio", hostAndPort, name, role, next.getMemFragmentationRatio());
			setLong("mem_fragmentation_bytes", hostAndPort, name, role, next.getMemFragmentationBytes());
			
			// command
			setLong("total_connections_received", hostAndPort, name, role, next.getTotalConnectionsReceived());
			setLong("total_commands_processed", hostAndPort, name, role, next.getTotalCommandsProcessed());
			
			setLong("total_reads_processed", hostAndPort, name, role, next.getTotalReadsProcessed());
			setLong("total_writes_processed", hostAndPort, name, role, next.getTotalWritesProcessed());
			setLong("total_error_replies", hostAndPort, name, role, next.getTotalErrorReplies());
			
			Long hits = next.getKeyspaceHits();
			Long misses = next.getKeyspaceMisses();
			if (hits != null && misses != null && (hits + misses) > 0) {
				setDouble("keyspace_hit_rate", hostAndPort, name, role, hits * 1d / (hits + misses));
			}
			
			// ops
			setLong("total_net_input_bytes", hostAndPort, name, role, next.getTotalNetInputBytes());
			setLong("total_net_output_bytes", hostAndPort, name, role, next.getTotalNetOutputBytes());
			setDouble("evicted_keys_per_sec", hostAndPort, name, role, next.getEvictedKeysPerSec());
			setDouble("instantaneous_ops_per_sec", hostAndPort, name, role, next.getInstantaneousOpsPerSec());
			setDouble("instantaneous_write_ops_per_sec", hostAndPort, name, role, next.getInstantaneousWriteOpsPerSec());
			setDouble("instantaneous_read_ops_per_sec", hostAndPort, name, role, next.getInstantaneousReadOpsPerSec());
			setDouble("instantaneous_other_ops_per_sec", hostAndPort, name, role, next.getInstantaneousOtherOpsPerSec());
			setDouble("instantaneous_sync_write_ops_per_sec", hostAndPort, name, role, next.getInstantaneousSyncWriteOpsPerSec());
			setDouble("instantaneous_input_kbps", hostAndPort, name, role, next.getInstantaneousInputKbps());
			setDouble("instantaneous_output_kbps", hostAndPort, name, role, next.getInstantaneousOutputKbps());
			
			// cpu
			setDouble("used_cpu_sys", hostAndPort, name, role, next.getUsedCpuSys());
			setDouble("used_cpu_user", hostAndPort, name, role, next.getUsedCpuUser());
			setDouble("used_cpu_sys_children", hostAndPort, name, role, next.getUsedCpuSysChildren());
			setDouble("used_cpu_user_children", hostAndPort, name, role, next.getUsedCpuUserChildren());
			
			// db
			for (Map.Entry<String, Long> entry : next.getDbInfo().entrySet()) {
				monitor.set("dbnum", hostAndPort, name, role, entry.getKey(), entry.getValue());
			}
			for (Map.Entry<String, Long> entry : next.getDbExpireInfo().entrySet()) {
				monitor.set("dbexp", hostAndPort, name, role, entry.getKey(), entry.getValue());
			}
			
			// diff
			setLong("expired_keys", hostAndPort, name, role, next.getExpiredKeys());
			setLong("evicted_keys", hostAndPort, name, role, next.getEvictedKeys());
			
			// slow log
			setLong("total_slow_log", hostAndPort, name, role, next.getTotalSlowLog());
			
			List<XSlowLog> slowLogs = next.getDiffSlowLogs();
			for (XSlowLog slowLog : slowLogs) {
				String[] properties = new String[8];
				properties[0] = hostAndPort;
				properties[1] = name;
				properties[2] = role;
				properties[3] = String.valueOf(slowLog.getId());
				properties[4] = slowLog.getTimestamp();
				properties[5] = slowLog.getCommand();
				properties[6] = slowLog.getClientName();
				properties[7] = slowLog.getHostAndPort();
				monitor.set("slow_log", properties, slowLog.getExecutionTime());
			}
			
			if (next.getDiffTotalSlowLog() > 0) {
				setDouble("slow_log_latency", hostAndPort, name, role, (next.getDiffTotalSlowLogExecutionTime() / (next.getDiffTotalSlowLog() * 1d)));
			} else {
				setDouble("slow_log_latency", hostAndPort, name, role, 0d);
			}
			
			if (Strings.isEquals(next.getRole(), "master")) {
				List<HostAndPort> prevs = prev.getSlaves();
				List<HostAndPort> nexts = next.getSlaves();
				
				for (HostAndPort hp : nexts) {
					if(!prevs.contains(hp)) {
						notifyUp(hp);
					}
				}
				
				for (HostAndPort hp : prevs) {
					if (!nexts.contains(hp)) {
						notifyDown(hp);
					}
				}
			} else {
				if (Strings.isEquals(next.getMasterStatus(), "down")) {
					notifyDown(next.getMaster());
				} else {
					notifyUp(next.getMaster());
				}
			}
			
			prev = next;
			monitor.set("redis_status", hostAndPort, name, "ok");
		} catch (Throwable e) {
			logger.error("error", e);
			monitor.set("redis_status", hostAndPort, name, "down");
			try {
				this.endpoint = XEndpoint.valueOf(this.endpoint, 0);
			} catch (Throwable ignore) {
			}
		}
	}
	
	private void setLong(String field, String hostAndPort, String name, String role, Long value) {
		if (value != null) {
			monitor.set(field, hostAndPort, name, role, value);
		}
	}
	
	private void setDouble(String field, String hostAndPort, String name, String role, Double value) {
		if (value != null) {
			monitor.set(field, hostAndPort, name, role, value);
		}
	}
	
	private void setString(String field, String hostAndPort, String name, String role, String value) {
		if (value != null) {
			monitor.set(field, hostAndPort, name, role, value);
		}
	}
	
	@Override
	public void close() {
		XEndpoint.closeQuietly(endpoint);
	}
}
