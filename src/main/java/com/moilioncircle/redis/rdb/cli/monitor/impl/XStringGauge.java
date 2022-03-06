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

import java.util.concurrent.atomic.AtomicReference;

import com.moilioncircle.redis.rdb.cli.monitor.Gauge;

/**
 * @author Baoyi Chen
 */
public class XStringGauge implements Gauge<String> {
	private final AtomicReference<String> gauge = new AtomicReference<>();
	
	@Override
	public String getGauge() {
		return this.gauge.get();
	}
	
	@Override
	public XStringGauge reset() {
		String v = gauge.getAndSet(null);
		if (v == null) {
			return null;
		} else {
			return new ImmutableXStringGauge(v);
		}
	}
	
	void set(String value) {
		gauge.set(value);
	}
	
	private static class ImmutableXStringGauge extends XStringGauge {
		private final String value;
		
		public ImmutableXStringGauge(String v) {
			this.value = v;
		}
		
		@Override
		public XStringGauge reset() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public String getGauge() {
			return value;
		}
	}
}
