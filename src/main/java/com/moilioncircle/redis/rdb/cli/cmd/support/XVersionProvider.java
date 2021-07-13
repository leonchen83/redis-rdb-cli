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

package com.moilioncircle.redis.rdb.cli.cmd.support;

import static com.moilioncircle.redis.rdb.cli.cmd.support.Version.INSTANCE;

import com.moilioncircle.redis.rdb.cli.util.Strings;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
public class XVersionProvider implements CommandLine.IVersionProvider {
	
	@Override
	public String[] getVersion() throws Exception {
		StringBuilder builder = new StringBuilder();
		builder.append("redis rdb cli: ");
		if (INSTANCE.version() != null) {
			builder.append(INSTANCE.version());
		}
		if (!Strings.isEmpty(INSTANCE.commit())) {
			builder.append(" (").append(INSTANCE.commit()).append(": ");
			builder.append(INSTANCE.date()).append(")");
		}
		builder.append("\n");
		if (!Strings.isEmpty(INSTANCE.home())) {
			builder.append(" home: ").append(INSTANCE.home()).append("\n");
		}
		builder.append("java version: ").append(System.getProperty("java.version")).append(", ");
		builder.append("vendor: ").append(System.getProperty("java.vendor")).append("\n");
		builder.append("java home: ").append(System.getProperty("java.home")).append("\n");
		builder.append("default locale: ").append(System.getProperty("user.language")).append(", ");
		builder.append("platform encoding: ").append(System.getProperty("file.encoding")).append("\n");
		builder.append("os name: ").append(System.getProperty("os.name")).append(", ");
		builder.append("version: ").append(System.getProperty("os.version")).append(", ");
		builder.append("arch: ").append(System.getProperty("os.arch"));
		return new String[]{builder.toString()};
	}
}
