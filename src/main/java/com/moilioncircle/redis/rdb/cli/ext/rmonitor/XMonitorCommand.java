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
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.impl.XMonitorCluster;
import com.moilioncircle.redis.rdb.cli.ext.rmonitor.impl.XMonitorMasterSlave;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.sentinel.RedisSentinelURI;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;

import redis.clients.jedis.HostAndPort;

/**
 * @author Baoyi Chen
 */
public class XMonitorCommand extends AbstractMonitorCommand {
	
	private MonitorCommand command;
	
	public XMonitorCommand(String source, String name, Configure configure) throws Exception {
		super(name, configure);
		try {
			RedisURI uri = new RedisURI(source);
			String host = uri.getHost();
			int port = uri.getPort();
			Configuration configuration = configure.merge(uri, true);
			try (XEndpoint endpoint = new XEndpoint(host, port, configuration)) {
				RedisObject r = endpoint.send(CLUSTER, NODES);
				if (r.type.isError()) {
					command = new XMonitorMasterSlave(uri.getHost(), uri.getPort(), name, monitor, configuration);
				} else {
					command = new XMonitorCluster(r.getString(), name, monitor, configuration);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} catch (URISyntaxException e) {
			RedisSentinelURI uri = new RedisSentinelURI(source);
			String masterName = uri.getParameters().get("master");
			Configuration configuration = configure.merge(uri, true);
			configuration.setAuthUser(uri.getUser()).setAuthPassword(uri.getPassword());
			
			for (HostAndPort sentinel : uri.getHosts()) {
				try (XEndpoint endpoint = new XEndpoint(sentinel.getHost(), sentinel.getPort(), -1, 1, false, configuration)) {
					RedisObject r = endpoint.send("SENTINEL".getBytes(), "GET-MASTER-ADDR-BY-NAME".getBytes(), masterName.getBytes());
					RedisObject[] ary = r.getArray();
					String host = ary[0].getString();
					int port = Integer.parseInt(ary[1].getString());
					command = new XMonitorMasterSlave(host, port, name, monitor, configure.merge(uri, true));
					break;
				} catch (IOException ignore) {
				}
			}
			if (command == null) {
				throw new RuntimeException("failed to connect all sentinel hosts. uri:" + source);
			}
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
		delay(configure.getMonitorRefreshInterval(), TimeUnit.MILLISECONDS);
	}
}
