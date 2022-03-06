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

package com.moilioncircle.redis.rdb.cli.glossary;

/**
 * @author Baoyi Chen
 */
public enum FileType {
	CSV("csv"),
	JSONL("jsonl");
	
	private String value;
	
	FileType(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return this.value;
	}
	
	public static FileType parse(String value) {
		if (value.equals("csv")) return CSV;
		else if (value.equals("jsonl")) return JSONL;
		else throw new UnsupportedOperationException(value);
	}
}
