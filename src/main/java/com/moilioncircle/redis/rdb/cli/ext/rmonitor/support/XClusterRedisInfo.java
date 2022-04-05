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

import java.util.List;

import com.moilioncircle.redis.rdb.cli.conf.XClusterNodes;

/**
 * @author Baoyi Chen
 */
public class XClusterRedisInfo {
	
	public static XClusterRedisInfo EMPTY_CLUSTER = new XClusterRedisInfo();
	
	//
	private XClusterNodes clusterNodes = new XClusterNodes();
	private XClusterInfo clusterInfo = new XClusterInfo();
	
	public XClusterInfo getClusterInfo() {
		return clusterInfo;
	}
	
	public void setClusterInfo(XClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}
	
	public XClusterNodes getClusterNodes() {
		return clusterNodes;
	}
	
	public void setClusterNodes(XClusterNodes clusterNodes) {
		this.clusterNodes = clusterNodes;
	}
	
	public static XClusterRedisInfo valueOf(List<XClusterNodes> clusterNodes, List<XClusterInfo> clusterInfos) {
		XClusterRedisInfo xinfo = new XClusterRedisInfo();
		// 
		for (XClusterNodes temp : clusterNodes) {
			if (xinfo.clusterNodes.getCurrentEpoch() < temp.getCurrentEpoch()) {
				xinfo.clusterNodes = temp;
			}
		}
		
		for (XClusterInfo clusterInfo : clusterInfos) {
			if (xinfo.clusterInfo.getClusterCurrentEpoch() < clusterInfo.getClusterCurrentEpoch()) {
				xinfo.clusterInfo.copy(clusterInfo);
			}
			xinfo.clusterInfo.setClusterStatsMessagesSent(add(xinfo.clusterInfo.getClusterStatsMessagesSent(), clusterInfo.getClusterStatsMessagesSent()));
			xinfo.clusterInfo.setClusterStatsMessagesReceived(add(xinfo.clusterInfo.getClusterStatsMessagesReceived(), clusterInfo.getClusterStatsMessagesReceived()));
		}
		return xinfo;
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
	
	@Override
	public String toString() {
		return "XClusterRedisInfo{" +
				"clusterInfo=" + clusterInfo +
				", clusterNodes=" + clusterNodes +
				'}';
	}
}
