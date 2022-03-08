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
public class Measures {
	
	public static final String ENDPOINT_SEND = "endpoint_send";
	public static final String ENDPOINT_RECONNECT = "endpoint_reconnect";
	public static final String ENDPOINT_FAILURE = "endpoint_failure";
	public static final String ENDPOINT_SUCCESS = "endpoint_success";
	
	public static final String[] ENDPOINT_MEASUREMENTS = new String[] {ENDPOINT_SEND, ENDPOINT_RECONNECT, ENDPOINT_FAILURE, ENDPOINT_SUCCESS};
	
	public static final String MEMORY_BIG_KEY = "memory_big_key";
	public static final String MEMORY_DB_NUMBERS = "memory_dbnum";
	public static final String MEMORY_DB_EXPIRES = "memory_dbexp";
	public static final String MEMORY_TYPE_COUNT = "memory_type_count";
	public static final String MEMORY_TYPE_MEMORY = "memory_type_memory";
	public static final String MEMORY_TOTAL_MEMORY = "memory_total_memory";
	
	public static final String[] MEMORY_MEASUREMENTS = new String[] {MEMORY_DB_NUMBERS, MEMORY_DB_EXPIRES, MEMORY_TYPE_COUNT, MEMORY_TYPE_MEMORY, MEMORY_TOTAL_MEMORY, MEMORY_BIG_KEY};
}
