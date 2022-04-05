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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Baoyi Chen
 */
public class XClusterInfo {
	private String clusterState;
	private long clusterSlotsAssigned;
	private long clusterSlotsOk;
	private long clusterSlotsPfail;
	private long clusterSlotsFail;
	private long clusterKnownNodes;
	private long clusterSize;
	private long clusterCurrentEpoch;
	private long clusterStatsMessagesSent;
	private long clusterStatsMessagesReceived;
	
	public String getClusterState() {
		return clusterState;
	}
	
	public void setClusterState(String clusterState) {
		this.clusterState = clusterState;
	}
	
	public long getClusterSlotsAssigned() {
		return clusterSlotsAssigned;
	}
	
	public void setClusterSlotsAssigned(long clusterSlotsAssigned) {
		this.clusterSlotsAssigned = clusterSlotsAssigned;
	}
	
	public long getClusterSlotsOk() {
		return clusterSlotsOk;
	}
	
	public void setClusterSlotsOk(long clusterSlotsOk) {
		this.clusterSlotsOk = clusterSlotsOk;
	}
	
	public long getClusterSlotsPfail() {
		return clusterSlotsPfail;
	}
	
	public void setClusterSlotsPfail(long clusterSlotsPfail) {
		this.clusterSlotsPfail = clusterSlotsPfail;
	}
	
	public long getClusterSlotsFail() {
		return clusterSlotsFail;
	}
	
	public void setClusterSlotsFail(long clusterSlotsFail) {
		this.clusterSlotsFail = clusterSlotsFail;
	}
	
	public long getClusterKnownNodes() {
		return clusterKnownNodes;
	}
	
	public void setClusterKnownNodes(long clusterKnownNodes) {
		this.clusterKnownNodes = clusterKnownNodes;
	}
	
	public long getClusterSize() {
		return clusterSize;
	}
	
	public void setClusterSize(long clusterSize) {
		this.clusterSize = clusterSize;
	}
	
	public long getClusterCurrentEpoch() {
		return clusterCurrentEpoch;
	}
	
	public void setClusterCurrentEpoch(long clusterCurrentEpoch) {
		this.clusterCurrentEpoch = clusterCurrentEpoch;
	}
	
	public long getClusterStatsMessagesSent() {
		return clusterStatsMessagesSent;
	}
	
	public void setClusterStatsMessagesSent(long clusterStatsMessagesSent) {
		this.clusterStatsMessagesSent = clusterStatsMessagesSent;
	}
	
	public long getClusterStatsMessagesReceived() {
		return clusterStatsMessagesReceived;
	}
	
	public void setClusterStatsMessagesReceived(long clusterStatsMessagesReceived) {
		this.clusterStatsMessagesReceived = clusterStatsMessagesReceived;
	}
	
	public XClusterInfo copy(XClusterInfo that) {
		this.clusterState = that.clusterState;
		this.clusterSlotsAssigned = that.clusterSlotsAssigned;
		this.clusterSlotsOk = that.clusterSlotsOk;
		this.clusterSlotsPfail = that.clusterSlotsPfail;
		this.clusterSlotsFail = that.clusterSlotsFail;
		this.clusterKnownNodes = that.clusterKnownNodes;
		this.clusterSize = that.clusterSize;
		this.clusterCurrentEpoch = that.clusterCurrentEpoch;
		return this;
	}
	
	public static XClusterInfo valueOf(String clusterInfo) {
		Map<String, String> map = convert(clusterInfo);
		XClusterInfo xinfo = new XClusterInfo();
		xinfo.clusterState = getString("cluster_state", map);
		xinfo.clusterSlotsAssigned = getLong("cluster_slots_assigned", map);
		xinfo.clusterSlotsOk = getLong("cluster_slots_ok", map);
		xinfo.clusterSlotsPfail = getLong("cluster_slots_pfail", map);
		xinfo.clusterSlotsFail = getLong("cluster_slots_fail", map);
		xinfo.clusterKnownNodes = getLong("cluster_known_nodes", map);
		xinfo.clusterSize = getLong("cluster_size", map);
		xinfo.clusterCurrentEpoch = getLong("cluster_current_epoch", map);
		xinfo.clusterStatsMessagesSent = getLong("cluster_stats_messages_sent", map);
		xinfo.clusterStatsMessagesReceived = getLong("cluster_stats_messages_received", map);
		return xinfo;
	}
	
	public static List<XClusterInfo> valueOf(List<String> clusterInfos) {
		List<XClusterInfo> result = new ArrayList<>(clusterInfos.size());
		for (String clusterInfo : clusterInfos) {
			result.add(valueOf(clusterInfo));
		}
		return result;
	}
	
	private static Map<String, String> convert(String info) {
		Map<String, String> map = new HashMap<>(16);
		String[] lines = info.split("\n");
		for (String line : lines) {
			line = line == null ? "" : line.trim();
			if (line.length() != 0) {
				String[] ary = line.split(":");
				if (ary.length == 2) {
					map.put(ary[0], ary[1]);
				}
			}
		}
		return map;
	}
	
	private static String getString(String key, Map<String, String> map) {
		if (map == null) return null;
		if (map.containsKey(key)) {
			return map.get(key);
		}
		return null;
	}
	
	private static long getLong(String key, Map<String, String> map) {
		if (map == null) return 0;
		if (map.containsKey(key)) {
			String value = map.get(key);
			try {
				return Long.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return "XClusterInfo{" +
				"clusterState='" + clusterState + '\'' +
				", clusterSlotsAssigned=" + clusterSlotsAssigned +
				", clusterSlotsOk=" + clusterSlotsOk +
				", clusterSlotsPfail=" + clusterSlotsPfail +
				", clusterSlotsFail=" + clusterSlotsFail +
				", clusterKnownNodes=" + clusterKnownNodes +
				", clusterSize=" + clusterSize +
				", clusterCurrentEpoch=" + clusterCurrentEpoch +
				", clusterStatsMessagesSent=" + clusterStatsMessagesSent +
				", clusterStatsMessagesReceived=" + clusterStatsMessagesReceived +
				'}';
	}
}
