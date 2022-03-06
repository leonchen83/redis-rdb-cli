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
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import com.moilioncircle.redis.rdb.cli.monitor.Counter;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public class XDoubleCounter implements Counter<Double> {
	private final AtomicReference<Slot> slot = new AtomicReference<>(new Slot());
	
	@Override
	public Tuple2<Double, Long> getCounter() {
		return this.slot.get().getCounter(false);
	}
	
	@Override
	public synchronized Counter<Double> reset() {
		Tuple2<Double, Long> v = slot.get().getCounter(true);
		return new ImmutableCounter(v);
	}
	
	void add(double count, long time) {
		Slot v = slot.get();
		v.add(count, time);
	}
	
	private static final class Slot {
		private final DoubleAdder v1 = new DoubleAdder();
		private final LongAdder v2 = new LongAdder();
		
		private void reset() {
			v1.reset();
			v2.reset();
		}
		
		private Tuple2<Double, Long> getCounter(boolean reset) {
			double n = reset ? v1.sumThenReset() : v1.sum();
			long t = reset ? v2.sumThenReset() : v2.sum();
			Tuple2<Double, Long> r = Tuples.of(n, t);
			return r;
		}
		
		private void add(double n, long t) {
			if (n > 0L) this.v1.add(n);
			if (t > 0L) this.v2.add(t);
		}
	}
	
	private static class ImmutableCounter implements Counter<Double> {
		private final Tuple2<Double, Long> value;
		
		private ImmutableCounter(Tuple2<Double, Long> value) {
			this.value = value;
		}
		
		@Override
		public Counter<Double> reset() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Tuple2<Double, Long> getCounter() {
			return Tuples.of(value.getV1(), value.getV2());
		}
	}
}
