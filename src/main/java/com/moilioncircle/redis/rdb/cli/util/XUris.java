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

import java.io.File;
import java.net.URISyntaxException;

import com.moilioncircle.redis.rdb.cli.sentinel.RedisSentinelURI;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisURI;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

/**
 * @author Baoyi Chen
 */
public abstract class XUris {
	
	public static String normalize(String source, FileType type, CommandSpec spec, String message) throws URISyntaxException {
		try {
			RedisSentinelURI uri = new RedisSentinelURI(source);
			if (uri != null) return uri.toString();
		} catch (Throwable e1) {
			RedisURI uri = null;
			try {
				uri = new RedisURI(source);
			} catch (Throwable e2) {
				try {
					uri = new RedisURI(new File(source));
				} catch (Throwable e3) {
				}
			}
			if (uri != null && (uri.getFileType() == null || uri.getFileType() == type)) {
				return uri.toString();
			}
		}
		throw new ParameterException(spec.commandLine(), message);
	}
	
}
