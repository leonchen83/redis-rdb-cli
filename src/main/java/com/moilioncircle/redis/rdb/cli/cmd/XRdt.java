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

import static com.moilioncircle.redis.rdb.cli.util.XUris.normalize;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.filter.XFilter;
import com.moilioncircle.redis.rdb.cli.glossary.Action;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unused")
@Command(name = "rdt",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: rdt [-hV] (-b <source> [-g <db>] | -s <source> -c <conf>",
				"       | -m <file>...) -o <file> [-d <db>...] [-k <regex>...]",
				"       [-t <type>...]"
		},
		description = "%nDescription: Backup, split and concatenate rdb snapshots files.",
		footer = {"%nExamples:",
				"  rdt -b ./dump.rdb -o ./dump.rdb1 -d 0 1",
				"  rdt -b ./dump.rdb -o ./dump.rdb1 -d 0 1 -g 3",
				"  rdt -b redis://127.0.0.1:6379 -o ./dump.rdb -k user.*",
				"  rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash",
				"  rdt -s ./dump.rdb -c ./nodes.conf -o /path/to/folder -t hash -d 0",
				"  rdt -s redis://127.0.0.1:6379 -c ./nodes.conf -o /path/to/folder -d 0"})
public class XRdt implements Callable<Integer> {
	
	@Spec
	private CommandSpec spec;
	
	@ArgGroup(exclusive = true, multiplicity = "1")
	private RdtExclusive exclusive;
	
	public static class RdtExclusive {
		@ArgGroup(exclusive = false)
		public Split split;
		
		@ArgGroup(exclusive = false)
		public Backup backup;
		
		@Option(names = {"-m", "--merge"}, arity = "1..*", required = true, paramLabel = "<file>", description = "Merge multi rdb files to one rdb file.", type = File.class)
		public List<File> merge;
	}
	
	public static class Backup {
		@Option(names = {"-b", "--backup"}, required = true, paramLabel = "<source>", description = {"Backup <source> to local rdb file. eg:", "/path/to/dump.rdb", "redis://host:port?authPassword=foobar", "redis:///path/to/dump.rdb"})
		public String backup;
		
		@Option(names = {"-g", "--goal"}, paramLabel = "<db>", description = {"Convert db from <source> and save to rdb", "file as <db>."}, type = Long.class)
		public Long goal;
	}
	
	public static class Split {
		@Option(names = {"-s", "--split"}, required = true, paramLabel = "<source>", description = {"Split rdb to multi rdb files via cluster's", "<nodes.conf>. eg:", "/path/to/dump.rdb", "redis://host:port?authPassword=foobar", "redis:///path/to/dump"})
		public String split;
		
		@Option(names = {"-c", "--config"}, required = true, paramLabel = "<conf>", description = {"Redis cluster's <nodes.conf> file(--split", "<source>)."}, type = File.class)
		public File config;
	}
	
	@Option(names = {"-o", "--out"}, required = true, paramLabel = "<file>", description = {"If --backup <source> or --merge <file>...","specified. the <file> is the target file.","if --split <source> specified. the <file>", "is the target path."}, type = File.class)
	private File output;
	
	@Option(names = {"-d", "--db"}, arity = "1..*", description = {"Database number. multiple databases can be", "provided. if not specified, all databases", "will be included."}, type = Integer.class)
	private List<Integer> db;
	
	@Option(names = {"-k", "--key"}, arity = "1..*", paramLabel = "<regex>", description = {"Keys to export. this can be a regex. if not", "specified, all keys will be returned."})
	private List<String> regexs;
	
	@Option(names = {"-t", "--type"}, arity = "1..*", description = {"Data type to export. possible values are", "string, hash, set, sortedset, list, module, ", "stream. multiple types can be provided. if not", "specified, all data types will be returned."})
	private List<String> type;
	
	@Option(names = {"-i", "--ignore-ttl"}, description = {"Ignore keys whose TTL is set, default is false."})
	private boolean ignoreTTL;
	
	@Override
	public Integer call() throws Exception {
		Action action = Action.NONE;
		// split
		File config = null;
		String split = null;
		
		// backup
		Long goal = null;
		String backup = null;
		
		// merge
		List<File> merge = null;
		
		Path path = output.toPath();
		if (exclusive.split != null && exclusive.split.split != null) {
			split = normalize(exclusive.split.split, FileType.RDB, spec, "Invalid options: '--split=<source>'");
			config = exclusive.split.config;
			if (Files.exists(path) && !Files.isDirectory(path)) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--out=<file>'");
			}
			
			action = Action.SPLIT;
		} else if (exclusive.backup != null && exclusive.backup.backup!= null) {
			backup = normalize(exclusive.backup.backup, FileType.RDB, spec, "Invalid options: '--backup=<backup>'");
			goal = exclusive.backup.goal;
			if (Files.exists(path) && !Files.isRegularFile(path)) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--out=<file>'");
			}
			
			action = Action.BACKUP;
		} else if (exclusive.merge != null) {
			merge = exclusive.merge;
			if (Files.exists(path) && !Files.isRegularFile(path)) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--out=<file>'");
			}
			
			action = Action.MERGE;
		}
		
		Configure configure = Configure.bind();
		try (ProgressBar bar = ProgressBar.bar(-1, configure.isEnableProgressBar())) {
			
			// bind args
			Args.RdtArgs arg = new Args.RdtArgs();
			arg.goal = goal;
			arg.split = split;
			arg.merge = merge;
			arg.config = config;
			arg.backup = backup;
			arg.output = output;
			arg.filter = XFilter.filter(regexs, db, type, ignoreTTL);
			
			List<Tuple2<Replicator, String>> list = action.dress(configure, arg);
			
			for (Tuple2<Replicator, String> tuple : list) {
				Replicator r = tuple.getV1();
				r.addEventListener((rep, event) -> {
					if (event instanceof PreRdbSyncEvent) {
						rep.addRawByteListener(b -> {
							bar.react(b.length, tuple.getV2());
						});
					}
					
					if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
						Replicators.closeQuietly(rep);
					}
					
				});
				r.open();
			}
		}
		
		return 0;
	}
}
