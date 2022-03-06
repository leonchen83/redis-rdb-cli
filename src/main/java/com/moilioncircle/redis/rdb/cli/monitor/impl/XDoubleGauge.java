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
public class XDoubleGauge implements Gauge<Double> {
	private final AtomicReference<Double> gauge = new AtomicReference<>(0d);
	
	@Override
	public Double getGauge() {
		return this.gauge.get();
	}
	
	@Override
	public XDoubleGauge reset() {
		double v = gauge.getAndSet(0d);
		if (v == 0) {
			return null;
		} else {
			return new ImmutableXDoubleGauge(v);
		}
	}
	
	void set(double value) {
		gauge.set(value);
	}
	
	private static class ImmutableXDoubleGauge extends XDoubleGauge {
		private final Double value;
		
		public ImmutableXDoubleGauge(Double v) {
			this.value = v;
		}
		
		@Override
		public XDoubleGauge reset() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Double getGauge() {
			return this.value;
		}
	}
}
