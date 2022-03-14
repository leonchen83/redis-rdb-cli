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

package com.moilioncircle.redis.rdb.cli.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class Collections {
	
	private static final List<?> EMPTY_LIST = new ArrayList<>(0);
	private static final Set<?> EMPTY_SET = new HashSet<>(0);
	
	public static <T> List<T> ofList(T...values) {
		if (values == null) {
			return (List<T>) EMPTY_LIST;
		}
		List<T> list = new ArrayList<>(values.length);
		for (T value : values) {
			list.add(value);
		}
		return list;
	}
	
	public static <T> Set<T> ofSet(T...values) {
		if (values == null) {
			return (Set<T>) EMPTY_SET;
		}
		Set<T> set = new HashSet<>(values.length);
		for (T value : values) {
			set.add(value);
		}
		return set;
	}
	
	public static <T> boolean isEmpty(List<T> list) {
		return list == null || list.isEmpty();
	}
	
	public static <T> boolean isEmpty(Set<T> list) {
		return list == null || list.isEmpty();
	}
}
