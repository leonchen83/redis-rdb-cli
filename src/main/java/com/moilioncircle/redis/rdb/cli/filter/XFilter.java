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

package com.moilioncircle.redis.rdb.cli.filter;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.moilioncircle.redis.rdb.cli.glossary.DataType;

/**
 * @author Baoyi Chen
 */
public class XFilter implements Filter {
	
	private static final List<Integer> DB0 = singletonList(0);
	
	private Set<String> keys;
	private List<Integer> dbs;
	private List<Pattern> regexs;
	private List<DataType> types;
	
	private XFilter() {
		this(null, null, null);
	}
	
	private XFilter(List<Integer> dbs) {
		this(null, dbs, null);
	}
	
	private XFilter(List<String> regexs, List<Integer> dbs, List<String> types) {
		
		if (regexs != null && !regexs.isEmpty()) {
			this.keys = new HashSet<>(regexs);
			this.regexs = regexs.stream().map(Pattern::compile).collect(toList());
		}
		
		this.dbs = dbs;
		
		if (types != null && !types.isEmpty()) {
			this.types = DataType.parse(types);
		}
	}
	
	public static Filter cluster() {
		return new XFilter(DB0);
	}
	
	public static Filter cluster(List<String> regexs, List<String> types) {
		return new XFilter(regexs, DB0, types);
	}
	
	public static Filter filter(List<Integer> dbs) {
		return new XFilter(null, dbs, null);
	}
	
	public static Filter filter(List<String> regexs, List<Integer> dbs, List<String> types) {
		return new XFilter(regexs, dbs, types);
	}
	
	private boolean containsDB(long db) {
		return dbs == null || dbs.isEmpty() || dbs.contains((int) db);
	}
	
	private boolean containsType(int type) {
		if (types == null || types.isEmpty()) {
			return true;
		}
		
		return DataType.contains(types, type);
	}
	
	private boolean containsKey(String key) {
		if (keys == null || keys.isEmpty() || keys.contains(key)) {
			return true;
		}
		for (Pattern pattern : regexs) {
			if (pattern.matcher(key).matches()) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public boolean contains(long db, int type, String key) {
		boolean r = containsDB(db) && containsType(type) && containsKey(key);
		return r;
	}
}
