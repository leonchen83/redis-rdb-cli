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

import static com.moilioncircle.redis.rdb.cli.ext.rct.support.JemallocCalculator.malloc;
import static com.moilioncircle.redis.rdb.cli.ext.rct.support.MemoryMisc.parseLong;
import static com.moilioncircle.redis.rdb.cli.ext.rct.support.MemoryMisc.power;

/**
 * @author Baoyi Chen
 */
public class MemoryCalculator {
	private int version;
	
	public MemoryCalculator(int version) {
		this.version = version;
	}
	
	public long calcListHeader() {
		return 48;
	}
	
	public long calcObjectHeader() {
		return 16;
	}
	
	public long calcSkipListHeader(long size) {
		return 48 + calcHashHeader(size);
	}
	
	public long calcQuickListHeader(long size) {
		return 32 + size * 48;
	}
	
	public long calcHashHeader(long size) {
		return (long) (92 + power(size) * 8 * 1.5);
	}
	
	public long calcString(byte[] bytes) {
		long[] num = parseLong(bytes);
		if (num[1] != -1) {
			if (num[0] < 10000) {
				return 0;
			} else {
				return 0;
			}
		}
		long len = bytes.length;
		if (version < 7) {
			return malloc(len + 9);
		} else if (len < (1L << 5)) {
			return malloc(len + 2);
		} else if (len < (1L << 8)) {
			return malloc(len + 4);
		} else if (len < (1L << 16)) {
			return malloc(len + 6);
		} else if (len < (1L << 32)) {
			return malloc(len + 10);
		} else {
			return malloc(len + 18);
		}
	}
	
	public long calcElement(byte[] element) {
		long[] num = parseLong(element);;
		if (num[1] != -1) {
			return 8;
		} else {
			return element.length;
		}
	}
	
	public long calcHashEntry() {
		return 24;
	}
	
	public long calcListEntry() {
		return 24;
	}
	
	public long calcSkipListEntry() {
		return 24 + calcHashEntry() + 16 * MemoryMisc.random();
	}
	
	public long calcObject(byte[] key, boolean expiry) {
		long value = calcHashEntry() + calcString(key) + calcObjectHeader();
		if (!expiry) return value; else return value + calcHashEntry() + 8;
	}
	
}