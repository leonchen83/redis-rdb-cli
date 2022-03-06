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

import java.util.concurrent.atomic.AtomicLong;

import com.moilioncircle.redis.rdb.cli.monitor.Gauge;

/**
 * @author Baoyi Chen
 */
public class XLongGauge implements Gauge<Long> {
	private final AtomicLong gauge = new AtomicLong(0);
	
	@Override
	public Long getGauge() {
		return this.gauge.get();
	}
	
	@Override
	public XLongGauge reset() {
		Long v = gauge.getAndSet(0);
		if (v == 0) {
			return null;
		} else {
			return new ImmutableXLongGauge(v);
		}
	}
	
	void set(long value) {
		gauge.set(value);
	}
	
	private static class ImmutableXLongGauge extends XLongGauge {
		private final Long value;
		
		public ImmutableXLongGauge(Long v) {
			this.value = v;
		}
		
		@Override
		public XLongGauge reset() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Long getGauge() {
			return value;
		}
	}
}
