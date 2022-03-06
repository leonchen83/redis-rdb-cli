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

package com.moilioncircle.redis.rdb.cli.monitor.points;

import java.util.Arrays;

import com.moilioncircle.redis.rdb.cli.monitor.Gauge;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorKey;

/**
 * @author Baoyi Chen
 */
public class GaugePoint<T> {
	private T value;
	private long timestamp;
	private String[] properties;
	private String monitorName;
	
	public T getValue() {
		return value;
	}
	
	public void setValue(T value) {
		this.value = value;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String[] getProperties() {
		return properties;
	}
	
	public void setProperties(String[] properties) {
		this.properties = properties;
	}
	
	public String getMonitorName() {
		return monitorName;
	}
	
	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}
	
	public static <T> GaugePoint<T> valueOf(Monitor monitor, MonitorKey key, Gauge<T> gauge) {
		GaugePoint<T> point = new GaugePoint<>();
		point.monitorName = key.getKey();
		point.properties = key.getProperties();
		point.timestamp = System.currentTimeMillis();
		point.value = gauge.getGauge();
		return point;
	}
	
	@Override
	public String toString() {
		return "GaugePoint{" +
				"monitorName='" + monitorName + '\'' +
				", properties=" + Arrays.toString(properties) +
				", timestamp=" + timestamp +
				", value=" + value +
				'}';
	}
}
