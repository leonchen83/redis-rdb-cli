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

package com.moilioncircle.redis.rdb.cli.ext.rct.support;

import static java.util.concurrent.ThreadLocalRandom.current;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.util.Strings;

/**
 * @author Baoyi Chen
 */
public class MemoryMisc {
	
	public static long power(long size) {
		long p = 1;
		long tmp = p;
		while (tmp <= size) {
			tmp <<= 1;
			if (tmp <= 0) {
				return p;
			} else {
				p = tmp;
			}
		}
		return p;
	}
	
	public static long[] parseLong(byte[] array) {
		long[] error = new long[]{-1L, -1L};
		if (array == null || array.length == 0) {
			return error;
		}
		
		int i = 0;
		long t = 0L;
		long sig = 1L;
		
		if (array[0] == '-') {
			sig = -1L;
			i = 1;
		} else if (array[0] == '+') {
			sig = 1L;
			i = 1;
		}
		
		for (; i < array.length; i++) {
			if (array[i] >= '0' && array[i] <= '9') {
				int x = array[i] - 48;
				t = t * 10 + x;
				if (t < 0) return error;
			} else {
				return error;
			}
		}
		return new long[]{t * sig, 1L};
	}
	
	public static long random() {
		long level = 1;
		int r = current().nextInt(0xFFFF);
		while (r < 0.25 * 0xFFFF) {
			level += 1;
			r = current().nextInt(0xFFFF);
		}
		return Math.min(level, 32);
	}
	
	public static String pretty(long value, Configure configure) {
		if (!configure.isExportUnit()) {
			return String.valueOf(value);
		} else {
			return Strings.pretty(value);
		}
	}
}
