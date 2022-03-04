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

import static com.moilioncircle.redis.rdb.cli.util.Outputs.newBufferedOutput;

import java.io.File;
import java.io.OutputStream;
import java.util.concurrent.Callable;

import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.CloseListener;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
@CommandLine.Command(name = "rcut",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: rcut [-hV] -s <source> -r <file> -a <file>",
		},
		footer = {"%nExamples:",
				"  rcut -s ./aof-use-rdb-preamble.aof -r ./dump.rdb -a ./appendonly.aof"})
public class XRcut implements Callable<Integer> {
	
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;
	
	@CommandLine.Option(names = {"-s", "--source"}, required = true, type = File.class, description = {"Source file that be cut. the file", "format MUST BE aof-use-rdb-preamble.", "eg: /path/to/appendonly.aof"})
	private File source;
	
	@CommandLine.Option(names = {"-r", "--rdb"}, required = true, paramLabel = "<file>", description = "Output rdb file.", type = File.class)
	private File rdb;
	
	@CommandLine.Option(names = {"-a", "--aof"}, required = true, paramLabel = "<file>", description = "Output aof file.", type = File.class)
	private File aof;
	
	@Override
	public Integer call() throws Exception {
		Configure configure = Configure.bind();
		OutputStream rdbStream = newBufferedOutput(rdb, configure.getOutputBufferSize());
		OutputStream aofStream = newBufferedOutput(aof, configure.getOutputBufferSize());
		
		RawByteListener rdbListener = new XRawByteListener(rdbStream);
		RawByteListener aofListener = new XRawByteListener(aofStream);
		
		try (ProgressBar bar = new ProgressBar(-1)) {
			
			Replicator r = new RedisReplicator(source, FileType.MIXED, Configuration.defaultSetting());
			
			r.addExceptionListener((rep, tx, e) -> {
				throw new RuntimeException(tx.getMessage(), tx);
			});
			
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				Replicators.closeQuietly(r);
			}));
			
			r.addEventListener((rep, event) -> {
				if (event instanceof PreRdbSyncEvent) {
					rep.addRawByteListener(b -> {
						bar.react(b.length);
					});
				}
			});
			
			r.addEventListener(new EventListener() {
				@Override
				public void onEvent(Replicator replicator, Event event) {
					if (event instanceof PreRdbSyncEvent) {
						replicator.addRawByteListener(rdbListener);
					}
					if (event instanceof PostRdbSyncEvent) {
						replicator.removeRawByteListener(rdbListener);
					}
					if (event instanceof PreCommandSyncEvent) {
						replicator.addRawByteListener(aofListener);
					}
					if (event instanceof PostCommandSyncEvent) {
						replicator.removeRawByteListener(aofListener);
					}
				}
			});
			r.addCloseListener(new CloseListener() {
				@Override
				public void handle(Replicator replicator) {
					Outputs.closeQuietly(rdbStream);
					Outputs.closeQuietly(aofStream);
				}
			});
			r.open();
		}
		return 0;
	}
	
	private static class XRawByteListener implements RawByteListener {
		
		private final OutputStream out;
		
		public XRawByteListener(OutputStream out) {
			this.out = out;
		}
		
		@Override
		public void handle(byte... bytes) {
			Outputs.writeQuietly(bytes, out);
		}
	}
}
