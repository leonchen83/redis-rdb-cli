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

import static com.moilioncircle.redis.rdb.cli.util.Collections.isEmpty;
import static java.util.stream.Collectors.toList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.util.Collections;

/**
 * @author Baoyi Chen
 */
public class XFilter implements Filter {
	
	private static final List<Integer> DB0 = Collections.ofList(0);
	
	private Set<String> keys;
	private List<Integer> dbs;
	private List<Pattern> regexs;
	private List<DataType> types;
	private boolean ignoreTTL = false;
	
	private XFilter() {
		this(null, null, null, false);
	}
	
	private XFilter(List<Integer> dbs) {
		this(null, dbs, null, false);
	}
	
	private XFilter(List<String> regexs, List<Integer> dbs, List<String> types, boolean ignoreTTL) {
		
		if (!isEmpty(regexs)) {
			this.keys = new HashSet<>(regexs);
			this.regexs = regexs.stream().map(Pattern::compile).collect(toList());
		}
		
		this.dbs = dbs;
		
		if (!isEmpty(types)) {
			this.types = DataType.parse(types);
		}
		
		this.ignoreTTL = ignoreTTL;
	}
	
	public static Filter cluster() {
		return new XFilter(DB0);
	}
	
	public static Filter cluster(List<String> regexs, List<String> types, boolean ignoreTTL) {
		return new XFilter(regexs, DB0, types, ignoreTTL);
	}
	
	public static Filter filter(List<Integer> dbs) {
		return new XFilter(null, dbs, null, false);
	}
	
	public static Filter filter(List<String> regexs, List<Integer> dbs, List<String> types, boolean ignoreTTL) {
		return new XFilter(regexs, dbs, types, ignoreTTL);
	}
	
	private boolean contains(int type) {
		if (isEmpty(types)) {
			return true;
		}
		
		return DataType.contains(types, type);
	}
	
	private boolean contains(String key) {
		if (isEmpty(keys) || keys.contains(key)) {
			return true;
		}
		for (Pattern pattern : regexs) {
			if (pattern.matcher(key).matches()) {
				return true;
			}
		}
		return false;
	}
	
	private boolean contains(boolean hasTTL) {
		if (hasTTL) return !ignoreTTL; else return true;
	}
	
	@Override
	public boolean contains(long db) {
		return isEmpty(dbs) || dbs.contains((int) db);
	}
	
	@Override
	public boolean contains(long db, int type, String key, boolean hasTTL) {
		return contains(db) && contains(type) && contains(key) && contains(hasTTL);
	}
}
