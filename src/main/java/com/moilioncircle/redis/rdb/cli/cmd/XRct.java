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

package com.moilioncircle.redis.rdb.cli.cmd;

import static com.moilioncircle.redis.rdb.cli.filter.XFilter.filter;
import static com.moilioncircle.redis.rdb.cli.util.XUris.normalize;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.XRedisReplicator;
import com.moilioncircle.redis.rdb.cli.glossary.Format;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.DefaultReplFilter;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unused")
@Command(name = "rct",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: rct [-hV] -f <format> -s <source> -o <file> [-e <escape>]",
				"       [-d <db>...] [-k <regex>...>] [-t <type>...] [-b <bytes>]",
				"       [-l <n>] [-r]"
		},
		description = "%nDescription: Convert rdb snapshot to other formats. Analyze memory usage by keys.",
		footer = {"%nExamples:",
				"  rct -f dump -s ./dump.rdb -o ./appendonly.aof -r",
				"  rct -f resp -s redis://127.0.0.1:6379 -o ./target.aof -d 0 1",
				"  rct -f json -s ./dump.rdb -o ./target.json -k user.* product.*",
				"  rct -f mem -s ./dump.rdb -o ./target.aof -e redis -t list -l 10 -b 1024"})
public class XRct implements Callable<Integer> {
	
	@Spec
	private CommandSpec spec;
	
	@Option(names = {"-f", "--format"}, required = true, description = {"Format to export. valid formats are json,", "jsonl, dump, diff, key, keyval, count, mem", "and resp"})
	private String format;
	
	@Option(names = {"-s", "--source"}, required = true, description = {"Source file or uri. eg:", "/path/to/dump.rdb", "redis://host:port?authPassword=foobar", "redis:///path/to/dump.rdb."})
	private String source;
	
	@Option(names = {"-o", "--out"}, required = true, paramLabel = "<file>", description = "Output file.", type = File.class)
	private File output;
	
	@Option(names = {"-d", "--db"}, arity = "1..*", description = {"Database number. multiple databases can be", "provided. if not specified, all databases", "will be included."}, type = Integer.class)
	private List<Integer> db;
	
	@Option(names = {"-k", "--key"}, arity = "1..*", paramLabel = "<regex>", description = {"Keys to export. this can be a regex. if not", "specified, all keys will be returned."})
	private List<String> regexs;
	
	@Option(names = {"-t", "--type"}, arity = "1..*", description = {"Data type to export. possible values are", "string, hash, set, sortedset, list, module, ", "stream. multiple types can be provided. if not", "specified, all data types will be returned."})
	private List<String> type;
	
	@Option(names = {"-e", "--escape"}, description = {"Escape strings to encoding: raw (default),", "redis, json."})
	private String escape;
	
	@Option(names = {"-b", "--bytes"}, description = {"Limit memory output(--format mem) to keys", "greater to or equal to this value (in bytes)"})
	private long bytes = -1L;
	
	@Option(names = {"-l", "--largest"}, paramLabel = "<n>", description = {"Limit memory output(--format mem) to only the", "top n keys (by size)."})
	private int largest = -1;
	
	@Option(names = {"-r", "--replace"}, description = {"Whether the generated aof with <replace>", "parameter(--format dump). if not specified,", "default value is false."})
	private boolean replace;
	
	@Option(names = {"-i", "--ignore-ttl"}, description = {"Ignore keys whose TTL is set, default is false."})
	private boolean ignoreTTL;
	
	@Override
	public Integer call() throws Exception {
		source = normalize(source, FileType.RDB, spec, "Invalid options: '--source=<source>'");
		Configure configure = Configure.bind();
		try (ProgressBar bar = ProgressBar.bar(-1, configure.isEnableProgressBar())) {
			// bind args
			Args.RctArgs args = new Args.RctArgs();
			args.bytes = bytes;
			args.output = output;
			args.replace = replace;
			args.largest = largest;
			args.filter = filter(regexs, db, type, ignoreTTL);
			
			Replicator r = new XRedisReplicator(source, configure, DefaultReplFilter.RDB);
			
			new Format(format).dress(r, configure, args, escape);
			
			r.addEventListener((rep, event) -> {
				if (event instanceof PreRdbSyncEvent) {
					rep.addRawByteListener(b -> {
						bar.react(b.length);
					});
				}
				
				if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
					Replicators.closeQuietly(rep);
				}
				
			});
			r.open();
		}
		return 0;
	}
}
