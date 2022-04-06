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

import java.util.concurrent.Callable;

import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.XMonitorCommand;
import com.moilioncircle.redis.rdb.cli.util.CloseableThread;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
@CommandLine.Command(name = "rmonitor",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: rmonitor [-hV] -s <uri> [-n <name>]"
		},
		footer = {"%nExamples:",
				"  rmonitor -s redis://127.0.0.1:6379 -n default"})
public class XRMonitor implements Callable<Integer> {
	
	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;
	
	@CommandLine.Option(names = {"-s", "--source"}, paramLabel = "<uri>", required = true, description = {"Source uri. eg: redis://host:port?authPassword=foobar."})
	private String source;
	
	@CommandLine.Option(names = {"-n", "--name"}, required = true, description = {"Monitor name."})
	private String name;
	
	@Override
	public Integer call() throws Exception {
		Configure configure = Configure.bind();
		XMonitorCommand command = new XMonitorCommand(source, name, configure);
		CloseableThread thread = CloseableThread.open("monitor", command, true);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			CloseableThread.close(thread);
			command.close();
		}));
		thread.join();
		return 0;
	}
}
