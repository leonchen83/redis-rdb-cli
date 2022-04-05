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
public class XClusterNode {
	
	private long pingTime;
	private long pongTime;
	private boolean master;
	private boolean myself;
	private String state;
	private long configEpoch;
	private List<Short> slots = new ArrayList<>(8192);
	private List<Short> migratingSlots = new ArrayList<>(8192);
	private String hostAndPort;
	private String name;
	private String link;
	
	public long getPingTime() {
		return pingTime;
	}
	
	public void setPingTime(long pingTime) {
		this.pingTime = pingTime;
	}
	
	public long getPongTime() {
		return pongTime;
	}
	
	public void setPongTime(long pongTime) {
		this.pongTime = pongTime;
	}
	
	public boolean isMaster() {
		return master;
	}
	
	public void setMaster(boolean master) {
		this.master = master;
	}
	
	public boolean isMyself() {
		return myself;
	}
	
	public void setMyself(boolean myself) {
		this.myself = myself;
	}
	
	public String getState() {
		return state;
	}
	
	public void setState(String state) {
		this.state = state;
	}
	
	public long getConfigEpoch() {
		return configEpoch;
	}
	
	public void setConfigEpoch(long configEpoch) {
		this.configEpoch = configEpoch;
	}
	
	public List<Short> getSlots() {
		return slots;
	}
	
	public void setSlots(List<Short> slots) {
		this.slots = slots;
	}
	
	public List<Short> getMigratingSlots() {
		return migratingSlots;
	}
	
	public void setMigratingSlots(List<Short> migratingSlots) {
		this.migratingSlots = migratingSlots;
	}
	
	public String getHostAndPort() {
		return hostAndPort;
	}
	
	public void setHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getLink() {
		return link;
	}
	
	public void setLink(String link) {
		this.link = link;
	}
	
	@Override
	public String toString() {
		return "XClusterNodes{" +
				"pingTime=" + pingTime +
				", pongTime=" + pongTime +
				", master=" + master +
				", myself=" + myself +
				", state='" + state + '\'' +
				", configEpoch=" + configEpoch +
				", slots=" + slots +
				", migratingSlots=" + migratingSlots +
				", hostAndPort=" + hostAndPort +
				", name='" + name + '\'' +
				", link='" + link + '\'' +
				'}';
	}
}
