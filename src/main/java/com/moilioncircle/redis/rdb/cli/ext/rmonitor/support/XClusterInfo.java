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
	private Long clusterSlotsAssigned;
	private Long clusterSlotsOk;
	private Long clusterSlotsPfail;
	private Long clusterSlotsFail;
	private Long clusterKnownNodes;
	private Long clusterSize;
	private Long clusterCurrentEpoch;
	private Long clusterStatsMessagesSent;
	private Long clusterStatsMessagesReceived;
	
	public String getClusterState() {
		return clusterState;
	}
	
	public void setClusterState(String clusterState) {
		this.clusterState = clusterState;
	}
	
	public Long getClusterSlotsAssigned() {
		return clusterSlotsAssigned;
	}
	
	public void setClusterSlotsAssigned(Long clusterSlotsAssigned) {
		this.clusterSlotsAssigned = clusterSlotsAssigned;
	}
	
	public Long getClusterSlotsOk() {
		return clusterSlotsOk;
	}
	
	public void setClusterSlotsOk(Long clusterSlotsOk) {
		this.clusterSlotsOk = clusterSlotsOk;
	}
	
	public Long getClusterSlotsPfail() {
		return clusterSlotsPfail;
	}
	
	public void setClusterSlotsPfail(Long clusterSlotsPfail) {
		this.clusterSlotsPfail = clusterSlotsPfail;
	}
	
	public Long getClusterSlotsFail() {
		return clusterSlotsFail;
	}
	
	public void setClusterSlotsFail(Long clusterSlotsFail) {
		this.clusterSlotsFail = clusterSlotsFail;
	}
	
	public Long getClusterKnownNodes() {
		return clusterKnownNodes;
	}
	
	public void setClusterKnownNodes(Long clusterKnownNodes) {
		this.clusterKnownNodes = clusterKnownNodes;
	}
	
	public Long getClusterSize() {
		return clusterSize;
	}
	
	public void setClusterSize(Long clusterSize) {
		this.clusterSize = clusterSize;
	}
	
	public Long getClusterCurrentEpoch() {
		return clusterCurrentEpoch;
	}
	
	public void setClusterCurrentEpoch(Long clusterCurrentEpoch) {
		this.clusterCurrentEpoch = clusterCurrentEpoch;
	}
	
	public Long getClusterStatsMessagesSent() {
		return clusterStatsMessagesSent;
	}
	
	public void setClusterStatsMessagesSent(Long clusterStatsMessagesSent) {
		this.clusterStatsMessagesSent = clusterStatsMessagesSent;
	}
	
	public Long getClusterStatsMessagesReceived() {
		return clusterStatsMessagesReceived;
	}
	
	public void setClusterStatsMessagesReceived(Long clusterStatsMessagesReceived) {
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
	
	private static Double getDouble(String key, Map<String, String> map) {
		if (map == null) return null;
		if (map.containsKey(key)) {
			String value = map.get(key);
			try {
				return Double.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return null;
	}
	
	private static Long getLong(String key, Map<String, String> map) {
		if (map == null) return null;
		if (map.containsKey(key)) {
			String value = map.get(key);
			try {
				return Long.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return null;
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
