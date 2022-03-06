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

package com.moilioncircle.redis.rdb.cli.monitor.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.moilioncircle.redis.rdb.cli.monitor.Counter;
import com.moilioncircle.redis.rdb.cli.monitor.Gauge;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorKey;


/**
 * @author Jingqi Xu
 */
public class XMonitor implements Monitor {
	//
	protected final String name;
	//
	private final Map<MonitorKey, XLongCounter> longCounters = new ConcurrentHashMap<>(8);
	private final Map<MonitorKey, XDoubleCounter> doubleCounters = new ConcurrentHashMap<>(8);
	//
	private final Map<MonitorKey, XLongGauge> longGauges = new ConcurrentHashMap<>(8);
	private final Map<MonitorKey, XDoubleGauge> doubleGauges = new ConcurrentHashMap<>(8);
	private final Map<MonitorKey, XStringGauge> stringGauges = new ConcurrentHashMap<>(8);
	
	public XMonitor(String name) {
		this.name = name;
	}
	
	@Override
	public final String getName() {
		return name;
	}
	
	@Override
	public void set(String measurement, long value) {
		this.doSetLong(measurement, null, value);
	}
	
	@Override
	public void set(String measurement, double value) {
		this.doSetDouble(measurement, null, value);
	}
	
	@Override
	public void set(String measurement, String value) {
		this.doSetString(measurement, null, value);
	}
	
	@Override
	public void set(String measurement, String property, long value) {
		this.doSetLong(measurement, new String[]{property}, value);
	}
	
	@Override
	public void set(String measurement, String property, double value) {
		this.doSetDouble(measurement, new String[]{property}, value);
	}
	
	@Override
	public void set(String measurement, String property, String value) {
		this.doSetString(measurement, new String[]{property}, value);
	}
	
	@Override
	public void set(String measurement, String[] properties, long value) {
		this.doSetLong(measurement, properties, value);
	}
	
	@Override
	public void set(String measurement, String[] properties, double value) {
		this.doSetDouble(measurement, properties, value);
	}
	
	@Override
	public void set(String measurement, String[] properties, String value) {
		this.doSetString(measurement, properties, value);
	}
	
	@Override
	public final void add(String measurement, long count) {
		this.doAddLong(measurement, null, count, 0);
	}
	
	@Override
	public final void add(String measurement, long count, long time) {
		this.doAddLong(measurement, null, count, time);
	}
	
	@Override
	public final void add(String measurement, String property, long count) {
		this.doAddLong(measurement, new String[]{property}, count, 0);
	}
	
	@Override
	public final void add(String measurement, String property, long count, long time) {
		this.doAddLong(measurement, new String[]{property}, count, time);
	}
	
	@Override
	public void add(String measurement, String[] properties, long count) {
		this.doAddLong(measurement, properties, count, 0);
	}
	
	@Override
	public void add(String measurement, String[] properties, long count, long time) {
		this.doAddLong(measurement, properties, count, time);
	}
	
	@Override
	public final void add(String measurement, double count) {
		this.doAddDouble(measurement, null, count, 0);
	}
	
	@Override
	public final void add(String measurement, double count, long time) {
		this.doAddDouble(measurement, null, count, time);
	}
	
	@Override
	public final void add(String measurement, String property, double count) {
		this.doAddDouble(measurement, new String[]{property}, count, 0);
	}
	
	@Override
	public final void add(String measurement, String property, double count, long time) {
		this.doAddDouble(measurement, new String[]{property}, count, time);
	}
	
	@Override
	public void add(String measurement, String[] properties, double count) {
		this.doAddDouble(measurement, properties, count, 0);
	}
	
	@Override
	public void add(String measurement, String[] properties, double count, long time) {
		this.doAddDouble(measurement, properties, count, time);
	}
	
	@Override
	public Map<MonitorKey, ? extends Counter<Long>> getLongCounters() {
		return this.longCounters;
	}
	
	@Override
	public Map<MonitorKey, ? extends Counter<Double>> getDoubleCounters() {
		return this.doubleCounters;
	}
	
	@Override
	public Map<MonitorKey, ? extends Gauge<Long>> getLongGauges() {
		return this.longGauges;
	}
	
	@Override
	public Map<MonitorKey, ? extends Gauge<Double>> getDoubleGauges() {
		return this.doubleGauges;
	}
	
	@Override
	public Map<MonitorKey, ? extends Gauge<String>> getStringGauges() {
		return this.stringGauges;
	}
	
	public static final class FactoryImpl implements Monitor.Factory {
		@Override
		public Monitor create(String name) {
			return new XMonitor(name);
		}
	}
	
	protected void doAddLong(String k, String[] p, long c, long t) {
		MonitorKey key = MonitorKey.key(k, p);
		XLongCounter x = longCounters.get(key);
		if (x == null) x = putIfAbsent(longCounters, key, new XLongCounter());
		x.add(c, t);
	}
	
	protected void doAddDouble(String k, String[] p, double c, long t) {
		MonitorKey key = MonitorKey.key(k, p);
		XDoubleCounter x = doubleCounters.get(key);
		if (x == null) x = putIfAbsent(doubleCounters, key, new XDoubleCounter());
		x.add(c, t);
	}
	
	protected void doSetLong(String k, String[] p, final long v) {
		MonitorKey key = MonitorKey.key(k, p);
		XLongGauge x = this.longGauges.get(key);
		if (x == null) x = putIfAbsent(longGauges, key, new XLongGauge());
		x.set(v);
	}
	
	protected void doSetDouble(String k, String[] p, final double v) {
		MonitorKey key = MonitorKey.key(k, p);
		XDoubleGauge x = this.doubleGauges.get(key);
		if (x == null) x = putIfAbsent(doubleGauges, key, new XDoubleGauge());
		x.set(v);
	}
	
	protected void doSetString(String k, String[] p, final String v) {
		MonitorKey key = MonitorKey.key(k, p);
		XStringGauge x = this.stringGauges.get(key);
		if (x == null) x = putIfAbsent(stringGauges, key, new XStringGauge());
		x.set(v);
	}
	
	private static final <K, V> V putIfAbsent(Map<K, V> m, K k, V v) {
		final V r = m.putIfAbsent(k, v);
		return r != null ? r : v;
	}
}
