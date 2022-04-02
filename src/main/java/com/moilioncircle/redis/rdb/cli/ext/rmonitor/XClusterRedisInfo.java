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

package com.moilioncircle.redis.rdb.cli.ext.rmonitor;

import static com.moilioncircle.redis.rdb.cli.ext.rmonitor.XStandaloneRedisInfo.EMPTY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moilioncircle.redis.rdb.cli.conf.NodeConfParser;
import com.moilioncircle.redis.rdb.cli.conf.XClusterNodes;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.support.XClusterInfo;

/**
 * @author Baoyi Chen
 */
public class XClusterRedisInfo {
	
	public static XClusterRedisInfo EMPTY_CLUSTER = new XClusterRedisInfo();
	
	//
	private XClusterInfo clusterInfo = new XClusterInfo();
	private List<XClusterInfo> clusterInfos = new ArrayList<>();
	private List<XClusterNodes> clusterNodes = new ArrayList<>();
	
	//
	private XStandaloneRedisInfo master = new XStandaloneRedisInfo();
	private XStandaloneRedisInfo slave = new XStandaloneRedisInfo();
	
	//
	private Map<String, XStandaloneRedisInfo> masters = new HashMap<>();
	private Map<String, XStandaloneRedisInfo> slaves = new HashMap<>();
	
	public XClusterInfo getClusterInfo() {
		return clusterInfo;
	}
	
	public void setClusterInfo(XClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}
	
	public List<XClusterInfo> getClusterInfos() {
		return clusterInfos;
	}
	
	public void setClusterInfos(List<XClusterInfo> clusterInfos) {
		this.clusterInfos = clusterInfos;
	}
	
	public List<XClusterNodes> getClusterNodes() {
		return clusterNodes;
	}
	
	public void setClusterNodes(List<XClusterNodes> clusterNodes) {
		this.clusterNodes = clusterNodes;
	}
	
	public XStandaloneRedisInfo getMaster() {
		return master;
	}
	
	public void setMaster(XStandaloneRedisInfo master) {
		this.master = master;
	}
	
	public XStandaloneRedisInfo getSlave() {
		return slave;
	}
	
	public void setSlave(XStandaloneRedisInfo slave) {
		this.slave = slave;
	}
	
	public Map<String, XStandaloneRedisInfo> getMasters() {
		return masters;
	}
	
	public void setMasters(Map<String, XStandaloneRedisInfo> masters) {
		this.masters = masters;
	}
	
	public Map<String, XStandaloneRedisInfo> getSlaves() {
		return slaves;
	}
	
	public void setSlaves(Map<String, XStandaloneRedisInfo> slaves) {
		this.slaves = slaves;
	}
	
	public static XClusterRedisInfo valueOf(List<XStandaloneRedisInfo> infos, String clusterNodes, List<String> clusterInfos) {
		XClusterRedisInfo xinfo = new XClusterRedisInfo();
		// 
		xinfo.clusterInfos = XClusterInfo.valueOf(clusterInfos);
		xinfo.clusterNodes = NodeConfParser.parse(clusterNodes);
		
		for (XStandaloneRedisInfo info : infos) {
			if (info.getRole().equals("master")) {
				xinfo.masters.put(info.getHostAndPort(), info);
				calculate(xinfo.master, info);
			} else if (info.getRole().equals("slave")) {
				xinfo.slaves.put(info.getHostAndPort(), info);
				calculate(xinfo.slave, info);
			}
		}
		for (XClusterInfo clusterInfo : xinfo.clusterInfos) {
			xinfo.clusterInfo.copy(clusterInfo);
			xinfo.clusterInfo.setClusterStatsMessagesSent(add(xinfo.clusterInfo.getClusterStatsMessagesSent(), clusterInfo.getClusterStatsMessagesSent()));
			xinfo.clusterInfo.setClusterStatsMessagesReceived(add(xinfo.clusterInfo.getClusterStatsMessagesReceived(), clusterInfo.getClusterStatsMessagesReceived()));
		}
		return xinfo;
	}
	
	public static XClusterRedisInfo diff(XClusterRedisInfo prev, XClusterRedisInfo next) {
		Map<String, XStandaloneRedisInfo> pmmap = prev.masters;
		Map<String, XStandaloneRedisInfo> nmmap = next.masters;
		next.masters = diff(pmmap, nmmap);
		
		Map<String, XStandaloneRedisInfo> psmap = prev.slaves;
		Map<String, XStandaloneRedisInfo> nsmap = next.slaves;
		next.slaves = diff(psmap, nsmap);
		
		for (Map.Entry<String, XStandaloneRedisInfo> entry : next.masters.entrySet()) {
			calculateDiff(next.master, entry.getValue());
		}
		
		for (Map.Entry<String, XStandaloneRedisInfo> entry : next.slaves.entrySet()) {
			calculateDiff(next.slave, entry.getValue());
		}
		
		return next;
	}
	
	private static Map<String, XStandaloneRedisInfo> diff(Map<String, XStandaloneRedisInfo> prev, Map<String, XStandaloneRedisInfo> next) {
		for (Map.Entry<String, XStandaloneRedisInfo> entry : next.entrySet()) {
			if (prev.containsKey(entry.getKey())) {
				XStandaloneRedisInfo.diff(prev.get(entry.getKey()), entry.getValue());
			} else {
				XStandaloneRedisInfo.diff(EMPTY, entry.getValue());
			}
		}
		return next;
	}
	
	private static void calculateDiff(XStandaloneRedisInfo result, XStandaloneRedisInfo info) {
		// diffTotalSlowLogExecutionTime
		// diffTotalSlowLog
		// diffSlowLogs
		result.setDiffTotalSlowLog(add(result.getDiffTotalSlowLog(), info.getDiffTotalSlowLog()));
		result.setDiffTotalSlowLogExecutionTime(add(result.getDiffTotalSlowLogExecutionTime(), info.getDiffTotalSlowLogExecutionTime()));
		result.getDiffSlowLogs().addAll(info.getDiffSlowLogs());
	}
	
	private static void calculate(XStandaloneRedisInfo result, XStandaloneRedisInfo info) {
		// uptimeInSeconds
		// String redisVersion
		// connectedClients
		// blockedClients
		// trackingClients
		// maxclients
		// maxmemory
		// usedMemory
		// usedMemoryRss
		// usedMemoryPeak
		// usedMemoryDataset
		// usedMemoryLua
		// usedMemoryFunctions
		// usedMemoryScripts
		// totalSystemMemory
		// memFragmentationBytes
		// totalConnectionsReceived
		// totalCommandsProcessed
		// totalReadsProcessed
		// totalWritesProcessed
		// totalErrorReplies
		// keyspaceHits
		// keyspaceMisses
		// totalNetInputBytes
		// totalNetOutputBytes
		// expiredKeys
		// evictedKeys
		// totalSlowLog
		// slowLogLen
		result.setRedisVersion(info.getRedisVersion());
		result.setUptimeInSeconds(max(result.getUptimeInSeconds(), info.getUptimeInSeconds()));
		result.setConnectedClients(add(result.getConnectedClients(), info.getConnectedClients()));
		result.setBlockedClients(add(result.getBlockedClients(), info.getBlockedClients()));
		result.setTrackingClients(add(result.getTrackingClients(), info.getTrackingClients()));
		result.setMaxclients(info.getMaxclients());
		result.setMaxmemory(add(result.getMaxmemory(), info.getMaxmemory()));
		result.setUsedMemory(add(result.getUsedMemory(), info.getUsedMemory()));
		result.setUsedMemoryRss(add(result.getUsedMemoryRss(), info.getUsedMemoryRss()));
		result.setUsedMemoryPeak(add(result.getUsedMemoryPeak(), info.getUsedMemoryPeak()));
		result.setUsedMemoryDataset(add(result.getUsedMemoryDataset(), info.getUsedMemoryDataset()));
		result.setUsedMemoryLua(add(result.getUsedMemoryLua(), info.getUsedMemoryLua()));
		result.setUsedMemoryFunctions(add(result.getUsedMemoryFunctions(), info.getUsedMemoryFunctions()));
		result.setUsedMemoryScripts(add(result.getUsedMemoryScripts(), info.getUsedMemoryScripts()));
		result.setTotalSystemMemory(add(result.getTotalSystemMemory(), info.getTotalSystemMemory()));
		result.setMemFragmentationBytes(add(result.getMemFragmentationBytes(), info.getMemFragmentationBytes()));
		result.setTotalConnectionsReceived(add(result.getTotalConnectionsReceived(), info.getTotalConnectionsReceived()));
		result.setTotalCommandsProcessed(add(result.getTotalCommandsProcessed(), info.getTotalCommandsProcessed()));
		result.setTotalReadsProcessed(add(result.getTotalReadsProcessed(), info.getTotalReadsProcessed()));
		result.setTotalWritesProcessed(add(result.getTotalWritesProcessed(), info.getTotalWritesProcessed()));
		result.setTotalErrorReplies(add(result.getTotalErrorReplies(), info.getTotalErrorReplies()));
		result.setKeyspaceHits(add(result.getKeyspaceHits(), info.getKeyspaceHits()));
		result.setKeyspaceMisses(add(result.getKeyspaceMisses(), info.getKeyspaceMisses()));
		result.setTotalNetInputBytes(add(result.getTotalNetInputBytes(), info.getTotalNetInputBytes()));
		result.setTotalNetOutputBytes(add(result.getTotalNetOutputBytes(), info.getTotalNetOutputBytes()));
		result.setExpiredKeys(add(result.getExpiredKeys(), info.getExpiredKeys()));
		result.setEvictedKeys(add(result.getEvictedKeys(), info.getEvictedKeys()));
		result.setTotalSlowLog(add(result.getTotalSlowLog(), info.getTotalSlowLog()));
		result.setSlowLogLen(info.getSlowLogLen());
	}
	
	private static long add(Long v1, Long v2) {
		if (v1 == null) {
			v1 = 0L;
		}
		if (v2 == null) {
			v2 = 0L;
		}
		return v1 + v2;
	}
	
	private static Long max(Long v1, Long v2) {
		if (v1 == null) {
			v1 = 0L;
		}
		if (v2 == null) {
			v2 = 0L;
		}
		return Math.max(v1, v2);
	}
	
	private static Long min(Long v1, Long v2) {
		if (v1 == null) {
			v1 = 0L;
		}
		if (v2 == null) {
			v2 = 0L;
		}
		return Math.min(v1, v2);
	}
	
	@Override
	public String toString() {
		return "XClusterRedisInfo{" +
				"clusterInfo=" + clusterInfo +
				", clusterInfos=" + clusterInfos +
				", clusterNodes=" + clusterNodes +
				", master=" + master +
				", slave=" + slave +
				", masters=" + masters +
				", slaves=" + slaves +
				'}';
	}
}
