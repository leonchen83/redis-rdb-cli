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

package com.moilioncircle.redis.rdb.cli.ext.rmonitor.support;

import static com.moilioncircle.redis.rdb.cli.util.Collections.isEmpty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.replicator.util.Strings;

import redis.clients.jedis.HostAndPort;

/**
 * @author Baoyi Chen
 */
public class XStandaloneRedisInfo {
	
	public static XStandaloneRedisInfo EMPTY = new XStandaloneRedisInfo();
	
	private HostAndPort master;
	private List<HostAndPort> slaves = new ArrayList<>();
	private String hostAndPort;
	private String role;
	private Long uptimeInSeconds;
	private String redisVersion;
	private Long connectedClients;
	private Long blockedClients;
	private Long trackingClients;
	private Long maxclients;
	private Long maxmemory;
	private Long usedMemory;
	private Long usedMemoryRss;
	private Long usedMemoryPeak;
	private Long usedMemoryDataset;
	private Long usedMemoryLua;
	private Long usedMemoryFunctions;
	private Long usedMemoryScripts;
	private Long totalSystemMemory;
	private Double memFragmentationRatio;
	private Long memFragmentationBytes;
	private Long totalConnectionsReceived;
	private Long totalCommandsProcessed;
	private Long totalReadsProcessed;
	private Long totalWritesProcessed;
	private Long totalErrorReplies;
	private Long keyspaceHits;
	private Long keyspaceMisses;
	private Long totalNetInputBytes;
	private Long totalNetOutputBytes;
	private Double evictedKeysPerSec;
	private Double instantaneousOpsPerSec;
	private Double instantaneousWriteOpsPerSec;
	private Double instantaneousReadOpsPerSec;
	private Double instantaneousOtherOpsPerSec;
	private Double instantaneousSyncWriteOpsPerSec;
	private Double instantaneousInputKbps;
	private Double instantaneousOutputKbps;
	private Double usedCpuSys;
	private Double usedCpuUser;
	private Double usedCpuSysChildren;
	private Double usedCpuUserChildren;
	private Long expiredKeys;
	private Long evictedKeys;
	private Long slowLogLen;
	private Long totalSlowLog;
	private List<XSlowLog> slowLogs = new ArrayList<>();
	private Map<String, Long> dbInfo = new HashMap<>();
	private Map<String, Long> dbExpireInfo = new HashMap<>();
	private Map<String, Long> commandStats = new HashMap<>(); 
	
	private Long diffTotalSlowLog;
	private Long diffTotalSlowLogExecutionTime;
	private List<XSlowLog> diffSlowLogs = new ArrayList<>();
	
	public HostAndPort getMaster() {
		return master;
	}
	
	public void setMaster(HostAndPort master) {
		this.master = master;
	}
	
	public List<HostAndPort> getSlaves() {
		return slaves;
	}
	
	public void setSlaves(List<HostAndPort> slaves) {
		this.slaves = slaves;
	}
	
	public String getHostAndPort() {
		return hostAndPort;
	}
	
	public void setHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}
	
	public Long getSlowLogLen() {
		return slowLogLen;
	}
	
	public void setSlowLogLen(Long slowLogLen) {
		this.slowLogLen = slowLogLen;
	}
	
	public String getRole() {
		return role;
	}
	
	public void setRole(String role) {
		this.role = role;
	}
	
	public Long getUptimeInSeconds() {
		return uptimeInSeconds;
	}
	
	public void setUptimeInSeconds(Long uptimeInSeconds) {
		this.uptimeInSeconds = uptimeInSeconds;
	}
	
	public String getRedisVersion() {
		return redisVersion;
	}
	
	public void setRedisVersion(String redisVersion) {
		this.redisVersion = redisVersion;
	}
	
	public Long getConnectedClients() {
		return connectedClients;
	}
	
	public void setConnectedClients(Long connectedClients) {
		this.connectedClients = connectedClients;
	}
	
	public Long getBlockedClients() {
		return blockedClients;
	}
	
	public void setBlockedClients(Long blockedClients) {
		this.blockedClients = blockedClients;
	}
	
	public Long getTrackingClients() {
		return trackingClients;
	}
	
	public void setTrackingClients(Long trackingClients) {
		this.trackingClients = trackingClients;
	}
	
	public Long getMaxclients() {
		return maxclients;
	}
	
	public void setMaxclients(Long maxclients) {
		this.maxclients = maxclients;
	}
	
	public Long getMaxmemory() {
		return maxmemory;
	}
	
	public void setMaxmemory(Long maxmemory) {
		this.maxmemory = maxmemory;
	}
	
	public Long getUsedMemory() {
		return usedMemory;
	}
	
	public void setUsedMemory(Long usedMemory) {
		this.usedMemory = usedMemory;
	}
	
	public Long getUsedMemoryRss() {
		return usedMemoryRss;
	}
	
	public void setUsedMemoryRss(Long usedMemoryRss) {
		this.usedMemoryRss = usedMemoryRss;
	}
	
	public Long getUsedMemoryPeak() {
		return usedMemoryPeak;
	}
	
	public void setUsedMemoryPeak(Long usedMemoryPeak) {
		this.usedMemoryPeak = usedMemoryPeak;
	}
	
	public Long getUsedMemoryDataset() {
		return usedMemoryDataset;
	}
	
	public void setUsedMemoryDataset(Long usedMemoryDataset) {
		this.usedMemoryDataset = usedMemoryDataset;
	}
	
	public Long getUsedMemoryLua() {
		return usedMemoryLua;
	}
	
	public void setUsedMemoryLua(Long usedMemoryLua) {
		this.usedMemoryLua = usedMemoryLua;
	}
	
	public Long getUsedMemoryFunctions() {
		return usedMemoryFunctions;
	}
	
	public void setUsedMemoryFunctions(Long usedMemoryFunctions) {
		this.usedMemoryFunctions = usedMemoryFunctions;
	}
	
	public Long getUsedMemoryScripts() {
		return usedMemoryScripts;
	}
	
	public void setUsedMemoryScripts(Long usedMemoryScripts) {
		this.usedMemoryScripts = usedMemoryScripts;
	}
	
	public Long getTotalSystemMemory() {
		return totalSystemMemory;
	}
	
	public void setTotalSystemMemory(Long totalSystemMemory) {
		this.totalSystemMemory = totalSystemMemory;
	}
	
	public Double getMemFragmentationRatio() {
		return memFragmentationRatio;
	}
	
	public void setMemFragmentationRatio(Double memFragmentationRatio) {
		this.memFragmentationRatio = memFragmentationRatio;
	}
	
	public Long getMemFragmentationBytes() {
		return memFragmentationBytes;
	}
	
	public void setMemFragmentationBytes(Long memFragmentationBytes) {
		this.memFragmentationBytes = memFragmentationBytes;
	}
	
	public Long getTotalConnectionsReceived() {
		return totalConnectionsReceived;
	}
	
	public void setTotalConnectionsReceived(Long totalConnectionsReceived) {
		this.totalConnectionsReceived = totalConnectionsReceived;
	}
	
	public Long getTotalCommandsProcessed() {
		return totalCommandsProcessed;
	}
	
	public void setTotalCommandsProcessed(Long totalCommandsProcessed) {
		this.totalCommandsProcessed = totalCommandsProcessed;
	}
	
	public Long getTotalReadsProcessed() {
		return totalReadsProcessed;
	}
	
	public void setTotalReadsProcessed(Long totalReadsProcessed) {
		this.totalReadsProcessed = totalReadsProcessed;
	}
	
	public Long getTotalWritesProcessed() {
		return totalWritesProcessed;
	}
	
	public void setTotalWritesProcessed(Long totalWritesProcessed) {
		this.totalWritesProcessed = totalWritesProcessed;
	}
	
	public Long getTotalErrorReplies() {
		return totalErrorReplies;
	}
	
	public void setTotalErrorReplies(Long totalErrorReplies) {
		this.totalErrorReplies = totalErrorReplies;
	}
	
	public Long getKeyspaceHits() {
		return keyspaceHits;
	}
	
	public void setKeyspaceHits(Long keyspaceHits) {
		this.keyspaceHits = keyspaceHits;
	}
	
	public Long getKeyspaceMisses() {
		return keyspaceMisses;
	}
	
	public void setKeyspaceMisses(Long keyspaceMisses) {
		this.keyspaceMisses = keyspaceMisses;
	}
	
	public Long getTotalNetInputBytes() {
		return totalNetInputBytes;
	}
	
	public void setTotalNetInputBytes(Long totalNetInputBytes) {
		this.totalNetInputBytes = totalNetInputBytes;
	}
	
	public Long getTotalNetOutputBytes() {
		return totalNetOutputBytes;
	}
	
	public void setTotalNetOutputBytes(Long totalNetOutputBytes) {
		this.totalNetOutputBytes = totalNetOutputBytes;
	}
	
	public Double getEvictedKeysPerSec() {
		return evictedKeysPerSec;
	}
	
	public void setEvictedKeysPerSec(Double evictedKeysPerSec) {
		this.evictedKeysPerSec = evictedKeysPerSec;
	}
	
	public Double getInstantaneousOpsPerSec() {
		return instantaneousOpsPerSec;
	}
	
	public void setInstantaneousOpsPerSec(Double instantaneousOpsPerSec) {
		this.instantaneousOpsPerSec = instantaneousOpsPerSec;
	}
	
	public Double getInstantaneousWriteOpsPerSec() {
		return instantaneousWriteOpsPerSec;
	}
	
	public void setInstantaneousWriteOpsPerSec(Double instantaneousWriteOpsPerSec) {
		this.instantaneousWriteOpsPerSec = instantaneousWriteOpsPerSec;
	}
	
	public Double getInstantaneousReadOpsPerSec() {
		return instantaneousReadOpsPerSec;
	}
	
	public void setInstantaneousReadOpsPerSec(Double instantaneousReadOpsPerSec) {
		this.instantaneousReadOpsPerSec = instantaneousReadOpsPerSec;
	}
	
	public Double getInstantaneousOtherOpsPerSec() {
		return instantaneousOtherOpsPerSec;
	}
	
	public void setInstantaneousOtherOpsPerSec(Double instantaneousOtherOpsPerSec) {
		this.instantaneousOtherOpsPerSec = instantaneousOtherOpsPerSec;
	}
	
	public Double getInstantaneousSyncWriteOpsPerSec() {
		return instantaneousSyncWriteOpsPerSec;
	}
	
	public void setInstantaneousSyncWriteOpsPerSec(Double instantaneousSyncWriteOpsPerSec) {
		this.instantaneousSyncWriteOpsPerSec = instantaneousSyncWriteOpsPerSec;
	}
	
	public Double getInstantaneousInputKbps() {
		return instantaneousInputKbps;
	}
	
	public void setInstantaneousInputKbps(Double instantaneousInputKbps) {
		this.instantaneousInputKbps = instantaneousInputKbps;
	}
	
	public Double getInstantaneousOutputKbps() {
		return instantaneousOutputKbps;
	}
	
	public void setInstantaneousOutputKbps(Double instantaneousOutputKbps) {
		this.instantaneousOutputKbps = instantaneousOutputKbps;
	}
	
	public Double getUsedCpuSys() {
		return usedCpuSys;
	}
	
	public void setUsedCpuSys(Double usedCpuSys) {
		this.usedCpuSys = usedCpuSys;
	}
	
	public Double getUsedCpuUser() {
		return usedCpuUser;
	}
	
	public void setUsedCpuUser(Double usedCpuUser) {
		this.usedCpuUser = usedCpuUser;
	}
	
	public Double getUsedCpuSysChildren() {
		return usedCpuSysChildren;
	}
	
	public void setUsedCpuSysChildren(Double usedCpuSysChildren) {
		this.usedCpuSysChildren = usedCpuSysChildren;
	}
	
	public Double getUsedCpuUserChildren() {
		return usedCpuUserChildren;
	}
	
	public void setUsedCpuUserChildren(Double usedCpuUserChildren) {
		this.usedCpuUserChildren = usedCpuUserChildren;
	}
	
	public Long getExpiredKeys() {
		return expiredKeys;
	}
	
	public void setExpiredKeys(Long expiredKeys) {
		this.expiredKeys = expiredKeys;
	}
	
	public Long getEvictedKeys() {
		return evictedKeys;
	}
	
	public void setEvictedKeys(Long evictedKeys) {
		this.evictedKeys = evictedKeys;
	}
	
	public Long getTotalSlowLog() {
		return totalSlowLog;
	}
	
	public void setTotalSlowLog(Long totalSlowLog) {
		this.totalSlowLog = totalSlowLog;
	}
	
	public List<XSlowLog> getSlowLogs() {
		return slowLogs;
	}
	
	public void setSlowLogs(List<XSlowLog> slowLogs) {
		this.slowLogs = slowLogs;
	}
	
	public List<XSlowLog> getDiffSlowLogs() {
		return diffSlowLogs;
	}
	
	public void setDiffSlowLogs(List<XSlowLog> diffSlowLogs) {
		this.diffSlowLogs = diffSlowLogs;
	}
	
	public Map<String, Long> getDbInfo() {
		return dbInfo;
	}
	
	public void setDbInfo(Map<String, Long> dbInfo) {
		this.dbInfo = dbInfo;
	}
	
	public Map<String, Long> getDbExpireInfo() {
		return dbExpireInfo;
	}
	
	public void setDbExpireInfo(Map<String, Long> dbExpireInfo) {
		this.dbExpireInfo = dbExpireInfo;
	}
	
	public Map<String, Long> getCommandStats() {
		return commandStats;
	}
	
	public void setCommandStats(Map<String, Long> commandStats) {
		this.commandStats = commandStats;
	}
	
	public Long getDiffTotalSlowLogExecutionTime() {
		return diffTotalSlowLogExecutionTime;
	}
	
	public void setDiffTotalSlowLogExecutionTime(Long diffTotalSlowLogExecutionTime) {
		this.diffTotalSlowLogExecutionTime = diffTotalSlowLogExecutionTime;
	}
	
	public Long getDiffTotalSlowLog() {
		return diffTotalSlowLog;
	}
	
	public void setDiffTotalSlowLog(Long diffTotalSlowLog) {
		this.diffTotalSlowLog = diffTotalSlowLog;
	}
	
	public static XStandaloneRedisInfo valueOf(String info, String maxclients, long slowLogLen, RedisObject[] binaryLogs, String hostAndPort) {
		Map<String, Map<String, String>> nextInfo = extract(info);
		XStandaloneRedisInfo xinfo = new XStandaloneRedisInfo();
		xinfo.hostAndPort = hostAndPort;
		xinfo.uptimeInSeconds = getLong("Server", "uptime_in_seconds", nextInfo);
		xinfo.redisVersion = getString("Server", "redis_version", nextInfo);
		xinfo.role = getString("Replication", "role", nextInfo);
		xinfo.connectedClients = getLong("Clients", "connected_clients", nextInfo);
		xinfo.blockedClients = getLong("Clients", "blocked_clients", nextInfo);
		xinfo.trackingClients = getLong("Clients", "tracking_clients", nextInfo);
		xinfo.maxclients = Long.parseLong(maxclients);
		xinfo.maxmemory = getLong("Memory", "maxmemory", nextInfo);
		xinfo.usedMemory = getLong("Memory", "used_memory", nextInfo);
		xinfo.usedMemoryRss = getLong("Memory", "used_memory_rss", nextInfo);
		xinfo.usedMemoryPeak = getLong("Memory", "used_memory_peak", nextInfo);
		xinfo.usedMemoryDataset = getLong("Memory", "used_memory_dataset", nextInfo);
		xinfo.usedMemoryLua = getLong("Memory", "used_memory_lua", nextInfo);
		xinfo.usedMemoryFunctions = getLong("Memory", "used_memory_functions", nextInfo);
		xinfo.usedMemoryScripts = getLong("Memory", "used_memory_scripts", nextInfo);
		xinfo.totalSystemMemory = getLong("Memory", "total_system_memory", nextInfo);
		xinfo.memFragmentationRatio = getDouble("Memory", "mem_fragmentation_ratio", nextInfo);
		xinfo.memFragmentationBytes = getLong("Memory", "mem_fragmentation_bytes", nextInfo);
		xinfo.totalConnectionsReceived = getLong("Stats", "total_connections_received", nextInfo);
		xinfo.totalCommandsProcessed = getLong("Stats", "total_commands_processed", nextInfo);
		xinfo.totalReadsProcessed = getLong("Stats", "total_reads_processed", nextInfo);
		xinfo.totalWritesProcessed = getLong("Stats", "total_writes_processed", nextInfo);
		xinfo.totalErrorReplies = getLong("Stats", "total_error_replies", nextInfo);
		xinfo.keyspaceHits = getLong("Stats", "keyspace_hits", nextInfo);
		xinfo.keyspaceMisses = getLong("Stats", "keyspace_misses", nextInfo);
		xinfo.expiredKeys = getLong("Stats", "expired_keys", nextInfo);
		xinfo.evictedKeys = getLong("Stats", "evicted_keys", nextInfo);
		xinfo.totalNetInputBytes = getLong("Stats", "total_net_input_bytes", nextInfo);
		xinfo.totalNetOutputBytes = getLong("Stats", "total_net_output_bytes", nextInfo);
		xinfo.evictedKeysPerSec = getDouble("Stats", "evicted_keys_per_sec", nextInfo);
		xinfo.instantaneousOpsPerSec = getDouble("Stats", "instantaneous_ops_per_sec", nextInfo);
		xinfo.instantaneousWriteOpsPerSec = getDouble("Stats", "instantaneous_write_ops_per_sec", nextInfo);
		xinfo.instantaneousReadOpsPerSec = getDouble("Stats", "instantaneous_read_ops_per_sec", nextInfo);
		xinfo.instantaneousOtherOpsPerSec = getDouble("Stats", "instantaneous_other_ops_per_sec", nextInfo);
		xinfo.instantaneousSyncWriteOpsPerSec = getDouble("Stats", "instantaneous_sync_write_ops_per_sec", nextInfo);
		xinfo.instantaneousInputKbps = getDouble("Stats", "instantaneous_input_kbps", nextInfo);
		xinfo.instantaneousOutputKbps = getDouble("Stats", "instantaneous_output_kbps", nextInfo);
		
		xinfo.usedCpuSys = getDouble("CPU", "used_cpu_sys", nextInfo);
		xinfo.usedCpuUser = getDouble("CPU", "used_cpu_user", nextInfo);
		xinfo.usedCpuSysChildren = getDouble("CPU", "used_cpu_sys_children", nextInfo);
		xinfo.usedCpuUserChildren = getDouble("CPU", "used_cpu_user_children", nextInfo);
		
		dbInfo(nextInfo, xinfo);
		commandStatInfo(nextInfo, xinfo);
		
		if (Strings.isEquals(xinfo.getRole(), "master")) {
			int slaves = getInt("Replication", "connected_slaves", nextInfo);
			for (int i = 0; i < slaves; i++) {
				String[] slaveInfo = getString("Replication", "slave" + i, nextInfo).split(",");
				String slaveHost = slaveInfo[0].split("=")[1];
				int slavePort = Integer.parseInt(slaveInfo[1].split("=")[1]);
				xinfo.slaves.add(new HostAndPort(slaveHost, slavePort));
			}
		} else {
			String masterHost = getString("Replication", "master_host", nextInfo);
			int masterPort = getInt("Replication", "master_port", nextInfo);
			xinfo.master = new HostAndPort(masterHost, masterPort);
		}
		
		xinfo.slowLogLen = slowLogLen;
		xinfo.slowLogs = XSlowLog.valueOf(binaryLogs, hostAndPort);
		xinfo.totalSlowLog = isEmpty(xinfo.slowLogs) ? 0 : xinfo.slowLogs.get(0).getId();
		return xinfo;
	}
	
	public static XStandaloneRedisInfo diff(XStandaloneRedisInfo prev, XStandaloneRedisInfo next) {
		diff(prev.slowLogs, next.slowLogs, next);
		return next;
	}
	
	public static void diff(List<XSlowLog> prev, List<XSlowLog> next, XStandaloneRedisInfo xinfo) {
		long nextId = isEmpty(next) ? 0 : next.get(0).getId();
		long prevId = isEmpty(prev) ? nextId : prev.get(0).getId();
		int count = (int) Math.min(nextId - prevId, xinfo.slowLogLen);
		xinfo.diffTotalSlowLog = (long) count;
		long totalExecutionTime = 0L;
		List<XSlowLog> logs = new ArrayList<>((int) count);
		for (int i = 0; i < count; i++) {
			XSlowLog log = next.get(i);
			totalExecutionTime += log.getExecutionTime();
			logs.add(log);
		}
		xinfo.diffSlowLogs = logs;
		xinfo.diffTotalSlowLogExecutionTime = totalExecutionTime;
	}
	
	private static String getString(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			return map.get(key).get(field);
		}
		return null;
	}
	
	private static Double getDouble(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			String value = map.get(key).get(field);
			try {
				return Double.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return null;
	}
	
	private static Long getLong(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			String value = map.get(key).get(field);
			try {
				return Long.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return null;
	}
	
	private static Integer getInt(String key, String field, Map<String, Map<String, String>> map) {
		if (map == null) return null;
		if (map.containsKey(key) && map.get(key).containsKey(field)) {
			String value = map.get(key).get(field);
			try {
				return Integer.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return null;
	}
	
	public static Map<String, Map<String, String>> extract(String info) {
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
	
	private static void dbInfo(Map<String, Map<String, String>> map, XStandaloneRedisInfo info) {
		try {
			Map<String, String> value = map.get("Keyspace");
			Map<String, Long> dbInfo = new HashMap<>(16);
			Map<String, Long> dbExpireInfo = new HashMap<>(16);
			for (Map.Entry<String, String> entry : value.entrySet()) {
				String key = entry.getKey();
				String[] ary = entry.getValue().split(",");
				long dbsize = Long.parseLong(ary[0].split("=")[1]);
				long expires = Long.parseLong(ary[1].split("=")[1]);
				dbInfo.put(key, dbsize);
				dbExpireInfo.put(key, expires);
			}
			info.dbInfo = dbInfo;
			info.dbExpireInfo = dbExpireInfo;
		} catch (NumberFormatException e) {
		}
	}
	
	private static void commandStatInfo(Map<String, Map<String, String>> map, XStandaloneRedisInfo info) {
		try {
			Map<String, String> value = map.get("Commandstats");
			Map<String, Long> commandStats = new HashMap<>(256);
			for (Map.Entry<String, String> entry : value.entrySet()) {
				String key = entry.getKey().split("_")[1];
				String[] ary = entry.getValue().split(",");
				long calls = Long.parseLong(ary[0].split("=")[1]);
				commandStats.put(key, calls);
			}
			info.commandStats = commandStats;
		} catch (NumberFormatException e) {
		}
	}
	
	@Override
	public String toString() {
		return "XStandaloneRedisInfo{" +
				"hostAndPort=" + hostAndPort +
				", role='" + role + '\'' +
				", uptimeInSeconds=" + uptimeInSeconds +
				", redisVersion='" + redisVersion + '\'' +
				", connectedClients=" + connectedClients +
				", blockedClients=" + blockedClients +
				", trackingClients=" + trackingClients +
				", maxclients=" + maxclients +
				", maxmemory=" + maxmemory +
				", usedMemory=" + usedMemory +
				", usedMemoryRss=" + usedMemoryRss +
				", usedMemoryPeak=" + usedMemoryPeak +
				", usedMemoryDataset=" + usedMemoryDataset +
				", usedMemoryLua=" + usedMemoryLua +
				", usedMemoryFunctions=" + usedMemoryFunctions +
				", usedMemoryScripts=" + usedMemoryScripts +
				", totalSystemMemory=" + totalSystemMemory +
				", memFragmentationRatio=" + memFragmentationRatio +
				", memFragmentationBytes=" + memFragmentationBytes +
				", totalConnectionsReceived=" + totalConnectionsReceived +
				", totalCommandsProcessed=" + totalCommandsProcessed +
				", totalReadsProcessed=" + totalReadsProcessed +
				", totalWritesProcessed=" + totalWritesProcessed +
				", totalErrorReplies=" + totalErrorReplies +
				", keyspaceHits=" + keyspaceHits +
				", keyspaceMisses=" + keyspaceMisses +
				", totalNetInputBytes=" + totalNetInputBytes +
				", totalNetOutputBytes=" + totalNetOutputBytes +
				", evictedKeysPerSec=" + evictedKeysPerSec +
				", instantaneousOpsPerSec=" + instantaneousOpsPerSec +
				", instantaneousWriteOpsPerSec=" + instantaneousWriteOpsPerSec +
				", instantaneousReadOpsPerSec=" + instantaneousReadOpsPerSec +
				", instantaneousOtherOpsPerSec=" + instantaneousOtherOpsPerSec +
				", instantaneousSyncWriteOpsPerSec=" + instantaneousSyncWriteOpsPerSec +
				", instantaneousInputKbps=" + instantaneousInputKbps +
				", instantaneousOutputKbps=" + instantaneousOutputKbps +
				", usedCpuSys=" + usedCpuSys +
				", usedCpuUser=" + usedCpuUser +
				", usedCpuSysChildren=" + usedCpuSysChildren +
				", usedCpuUserChildren=" + usedCpuUserChildren +
				", expiredKeys=" + expiredKeys +
				", evictedKeys=" + evictedKeys +
				", slowLogLen=" + slowLogLen +
				", totalSlowLog=" + totalSlowLog +
				", slowLogs=" + slowLogs +
				", dbInfo=" + dbInfo +
				", dbExpireInfo=" + dbExpireInfo +
				", diffTotalSlowLog=" + diffTotalSlowLog +
				", diffTotalSlowLogExecutionTime=" + diffTotalSlowLogExecutionTime +
				", diffSlowLogs=" + diffSlowLogs +
				'}';
	}
}
