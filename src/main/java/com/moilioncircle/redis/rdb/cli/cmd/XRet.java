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

import static com.moilioncircle.redis.rdb.cli.util.Iterators.find;
import static com.moilioncircle.redis.rdb.cli.util.XUris.normalize;

import java.io.File;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.api.sink.ParserService;
import com.moilioncircle.redis.rdb.cli.api.sink.SinkService;
import com.moilioncircle.redis.rdb.cli.api.sink.listener.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.XRedisReplicator;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.rdb.cli.util.XThreadFactory;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unused")
@Command(name = "ret",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: ret [-hV] -s <uri> [-c <conf>] [-p <parser>] -n <sink>"
		},
		description = "%nDescription: Run your own extension plugin.",
		footer = {"%nExamples:",
				"  ret -s redis://127.0.0.1:6379 -c ./config.conf -n example",
				"  ret -s redis://127.0.0.1:6379 -c ./config.conf -p dump -n example"})
public class XRet implements Callable<Integer> {
	
	private static final Logger logger = LoggerFactory.getLogger(XRet.class);
	
	@Spec
	private CommandSpec spec;
	
	@Option(names = {"-s", "--source"}, required = true, paramLabel = "<uri>", description = {"Redis uri. eg:", "redis://host:port?authPassword=foobar"})
	private String source;
	
	@Option(names = {"-c", "--config"}, required = false, paramLabel = "<conf>", description = {"External config file, if not specified,", "default value is null."}, type = File.class)
	private File config;
	
	@Option(names = {"-n", "--name"}, required = true, description = {"Sink service name, registered sink service:", "example."})
	private String sink;
	
	@Option(names = {"-p", "--parser"}, required = false, defaultValue = "default", description = {"Parser service name, registered parser ", "service: default, dump. if not specified,", "default value is default"})
	private String parser = "default";
	
	@Override
	public Integer call() throws Exception {
		source = normalize(source, null, spec, "Invalid options: '--source=<source>'");
		
		SinkService sinkService = loadSinkService(sink, config);
		ParserService parserService = loadParseService(parser, config);
		
		Configure configure = Configure.bind();
		
		try (ProgressBar bar = ProgressBar.bar(-1, configure.isEnableProgressBar())) {
			
			Replicator r = new XRedisReplicator(source, configure);
			r.setRdbVisitor(parserService.getRdbVisitor(r));
			
			r.addEventListener((rep, event) -> {
				if (event instanceof PreRdbSyncEvent) {
					rep.addRawByteListener(b -> {
						bar.react(b.length);
					});
				}
			});
			
			r.addEventListener(new AsyncEventListener(sinkService, r, configure.getMigrateThreads(), new XThreadFactory("sync-worker")));
			parserService.wrap(r).open();
		}
		return 0;
	}
	
	private SinkService loadSinkService(String sink, File config) throws Exception {
		ServiceLoader<SinkService> loader = ServiceLoader.load(SinkService.class);
		SinkService service = find(loader.iterator(), e -> e.sink().equals(sink));
		if (service == null) {
			throw new ParameterException(spec.commandLine(), "Failed to load sink service. Invalid options: '--name=<sink>'");
		}
		logger.info("loaded sink service {}", service.getClass());
		service.init(config);
		return service;
	}
	
	private ParserService loadParseService(String parser, File config) throws Exception {
		ServiceLoader<ParserService> loader = ServiceLoader.load(ParserService.class);
		ParserService service = find(loader.iterator(), e -> e.parser().equals(parser));
		if (service == null) {
			throw new ParameterException(spec.commandLine(), "Failed to load parser service. Invalid options: '--parser=<parser>'");
		}
		logger.info("loaded parser service {}", service.getClass());
		service.init(config);
		return service;
	}
}
