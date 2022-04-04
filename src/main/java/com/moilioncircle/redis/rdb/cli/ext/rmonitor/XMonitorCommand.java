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

package com.moilioncircle.redis.rdb.cli.ext.rmonitor;

import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.CLUSTER;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.NODES;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.impl.XMonitorCluster;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.impl.XMonitorMasterSlave;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;

/**
 * @author Baoyi Chen
 */
public class XMonitorCommand extends AbstractMonitorCommand {
	
	private MonitorCommand command;
	
	public XMonitorCommand(RedisURI uri, String name, Configure configure) {
		super(name, configure);
		String host = uri.getHost();
		int port = uri.getPort();
		Configuration configuration = configure.merge(uri, true);
		try(XEndpoint endpoint = new XEndpoint(host, port, 0, -1, false, configuration)) {
			RedisObject r = endpoint.send(CLUSTER, NODES);
			if (r.type.isError()) {
				command = new XMonitorMasterSlave(uri, name, monitor, configure);
			} else {
				command = new XMonitorCluster();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void close() {
		MonitorCommand.closeQuietly(command);
		super.close();
	}
	
	@Override
	public void run() {
		command.run();
		delay(15, TimeUnit.SECONDS);
	}
}
