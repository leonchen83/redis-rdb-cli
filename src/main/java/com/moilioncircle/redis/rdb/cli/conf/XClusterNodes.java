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

package com.moilioncircle.redis.rdb.cli.conf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class XClusterNodes {
	private long currentEpoch;
	private long lastVoteEpoch;
	private List<XClusterNode> nodes = new ArrayList<>();
	
	public long getCurrentEpoch() {
		return currentEpoch;
	}
	
	public void setCurrentEpoch(long currentEpoch) {
		this.currentEpoch = currentEpoch;
	}
	
	public long getLastVoteEpoch() {
		return lastVoteEpoch;
	}
	
	public void setLastVoteEpoch(long lastVoteEpoch) {
		this.lastVoteEpoch = lastVoteEpoch;
	}
	
	public List<XClusterNode> getNodes() {
		return nodes;
	}
	
	public void setNodes(List<XClusterNode> nodes) {
		this.nodes = nodes;
	}
	
	@Override
	public String toString() {
		return "XClusterNodes{" +
				"currentEpoch=" + currentEpoch +
				", lastVoteEpoch=" + lastVoteEpoch +
				", nodes=" + nodes +
				'}';
	}
}
