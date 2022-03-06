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

package com.moilioncircle.redis.rdb.cli.monitor;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author Baoyi Chen
 */
public class MonitorKey {
	//
	private String key;
	private String[] properties;
	
	public MonitorKey(String key, String... properties) {
		this.key = key;
		this.properties = properties;
	}
	
	public static MonitorKey key(String key, String... properties) {
		return new MonitorKey(key, properties);
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String[] getProperties() {
		return properties;
	}
	
	public void setProperties(String[] properties) {
		this.properties = properties;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		MonitorKey that = (MonitorKey) o;
		return key.equals(that.key) && Arrays.equals(properties, that.properties);
	}
	
	@Override
	public int hashCode() {
		int result = Objects.hash(key);
		result = 31 * result + Arrays.hashCode(properties);
		return result;
	}
}
