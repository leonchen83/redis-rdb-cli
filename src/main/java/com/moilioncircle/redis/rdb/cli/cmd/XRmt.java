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

import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.CLUSTER;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.NODES;
import static com.moilioncircle.redis.rdb.cli.filter.XFilter.cluster;
import static com.moilioncircle.redis.rdb.cli.filter.XFilter.filter;
import static com.moilioncircle.redis.rdb.cli.util.XUris.normalize;
import static java.nio.file.Files.readAllLines;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.XRedisReplicator;
import com.moilioncircle.redis.rdb.cli.ext.rmt.ClusterRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rmt.SingleRdbVisitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.util.Collections;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.DefaultReplFilter;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.RdbVisitor;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * @author Baoyi Chen
 */
@Command(name = "rmt",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: rmt [-hV] -s <source> (-m <uri> | -c <conf>) [-d <db>...]",
				"       [-k <regex>...] [-t <type>...] [-rl]"
		},
		footer = {"%nExamples:",
				"  rmt -s ./dump.rdb -c ./nodes.conf -t string -r",
				"  rmt -s ./dump.rdb -m redis://127.0.0.1:6380 -t list -d 0",
				"  rmt -s redis://127.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0"})
public class XRmt implements Callable<Integer> {
	
	@Spec
	private CommandSpec spec;
	
	@ArgGroup(exclusive = true, multiplicity = "1")
	private RmtExclusive exclusive;
	
	private static class RmtExclusive {
		@Option(names = {"-m", "--migrate"}, required = true, paramLabel = "<uri>", description = {"Migrate to uri. eg:", "redis://host:port?authPassword=foobar."})
		private String migrate;
		
		@Option(names = {"-c", "--config"}, required = true, paramLabel = "<conf>", description = {"Migrate data to cluster via redis cluster's", "<nodes.conf> file, if specified, no need to", "specify --migrate."}, type = File.class)
		private File config;
	}
	
	@Option(names = {"-s", "--source"}, required = true, description = {"Source file or uri. eg:", "/path/to/dump.rdb", "redis://host:port?authPassword=foobar", "redis:///path/to/dump.rdb."})
	private String source;
	
	@Option(names = {"-d", "--db"}, arity = "1..*", description = {"Database number. multiple databases can be", "provided. if not specified, all databases", "will be included."}, type = Integer.class)
	private List<Integer> db;
	
	@Option(names = {"-k", "--key"}, arity = "1..*", paramLabel = "<regex>", description = {"Keys to export. this can be a regex. if not", "specified, all keys will be returned."})
	private List<String> regexs;
	
	@Option(names = {"-t", "--type"}, arity = "1..*", description = {"Data type to export. possible values are", "string, hash, set, sortedset, list, module, ", "stream. multiple types can be provided. if not", "specified, all data types will be returned."})
	private List<String> type;
	
	@Option(names = {"-r", "--replace"}, description = {"Replace exist key value. if not specified,", "default value is false."})
	private boolean replace;
	
	@Option(names = {"-l", "--legacy"}, description = {"If specify the <replace> and this parameter.", "then use lua script to migrate data to target.", "if target redis version is greater than 3.0.", "no need to add this parameter."})
	private boolean legacy;
	
	@Override
	public Integer call() throws Exception {
		source = normalize(source, FileType.RDB, spec, "Invalid options: '--source=<source>'");
		Configure configure = Configure.bind();
		
		if (exclusive.migrate != null) {
			RedisURI uri = new RedisURI(exclusive.migrate);
			
			if (uri.getFileType() != null) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--migrate=<uri>'");
			}
			
			try (ProgressBar bar = ProgressBar.bar(-1, configure.isEnableProgressBar())) {
				
				Replicator r = new XRedisReplicator(source, configure, DefaultReplFilter.RDB);
				r.setRdbVisitor(getRdbVisitor(r, configure, uri));
				
				r.addEventListener((rep, event) -> {
					if (event instanceof PreRdbSyncEvent) {
						rep.addRawByteListener(b -> bar.react(b.length));
					}
					
					if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
						Replicators.closeQuietly(r);
					}
					
				});
				r.open();
			}
			
		} else {
			if (exclusive.config == null) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--config=<config>'");
			}
			Path path = exclusive.config.toPath();
			
			if (!Files.exists(path)) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--config=<config>'");
			}
			
			try (ProgressBar bar = ProgressBar.bar(-1, configure.isEnableProgressBar())) {
				
				Replicator r = new XRedisReplicator(source, configure, DefaultReplFilter.RDB);
				r.setRdbVisitor(new ClusterRdbVisitor(r, configure, cluster(regexs, type), null, readAllLines(path), replace));
				
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
		}
		return 0;
	}
	
	private RdbVisitor getRdbVisitor(Replicator replicator, Configure configure, RedisURI uri) throws Exception {
		try (XEndpoint endpoint = new XEndpoint(uri.getHost(), uri.getPort(), configure.merge(uri, false))) {
			RedisObject r = endpoint.send(CLUSTER, NODES);
			if (r.type.isError()) {
				return new SingleRdbVisitor(replicator, configure, filter(regexs, db, type), uri, replace, legacy);
			} else {
				List<String> lines = Collections.ofList(r.getString().split("\n"));
				return new ClusterRdbVisitor(replicator, configure, cluster(regexs, type), uri, lines, replace);
			}
		} catch (Throwable e) {
			throw new RuntimeException("failed to connect to " + uri.getHost() + ":" + uri.getPort() + ", reason " + e.getMessage());
		}
	}
}
