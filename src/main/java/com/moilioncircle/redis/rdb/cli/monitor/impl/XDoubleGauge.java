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
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public class XDoubleGauge implements Gauge<Double> {
	private final AtomicReference<Double> gauge = new AtomicReference<>(0d);
	private final AtomicReference<String> property = new AtomicReference<>();
	
	@Override
	public Tuple2<Double, String> getGauge() {
		return Tuples.of(this.gauge.get(), this.property.get());
	}
	
	@Override
	public XDoubleGauge reset() {
		Double v = gauge.getAndSet(0d);
		if (v == 0) {
			return null;
		} else {
			return new ImmutableXDoubleGauge(v, property.get());
		}
	}
	
	void set(double value) {
		gauge.set(value);
	}
	
	void setProperty(String value) {
		property.compareAndSet(null, value);
	}
	
	private static class ImmutableXDoubleGauge extends XDoubleGauge {
		private final Double value;
		private final String property;
		
		public ImmutableXDoubleGauge(Double v, String p) {
			this.value = v;
			this.property = p;
		}
		
		@Override
		public XDoubleGauge reset() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Tuple2<Double, String> getGauge() {
			return Tuples.of(this.value, this.property);
		}
	}
}
