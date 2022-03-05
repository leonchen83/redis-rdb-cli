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

import com.moilioncircle.redis.rdb.cli.monitor.Gauge;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;

/**
 * @author Baoyi Chen
 */
public class GaugePoint<T> {
	private T value;
	private long timestamp;
	private String property;
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
	
	public String getProperty() {
		return property;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public String getMonitorName() {
		return monitorName;
	}
	
	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}
	
	public static <T> GaugePoint<T> valueOf(Monitor monitor, String key, Gauge<T> gauge) {
		GaugePoint<T> point = new GaugePoint<>();
		point.monitorName = key;
		point.property = gauge.getGauge().getV2();
		point.timestamp = System.currentTimeMillis();
		point.value = gauge.getGauge().getV1();
		return point;
	}
	
	@Override
	public String toString() {
		return "GaugePoint{" +
				"monitorName='" + monitorName + '\'' +
				", property='" + property + '\'' +
				", timestamp=" + timestamp +
				", value=" + value +
				'}';
	}
}
