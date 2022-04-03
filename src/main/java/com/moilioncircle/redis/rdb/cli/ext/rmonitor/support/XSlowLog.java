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

package com.moilioncircle.redis.rdb.cli.ext.rmonitor.support;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.util.Collections;
import com.moilioncircle.redis.replicator.cmd.RedisCodec;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class XSlowLog {
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	private static RedisCodec codec = new RedisCodec();
	
	private Long id;
	private String command;
	private String timestamp;
	private Long executionTime;
	private String server = "";
	private String hostAndPort = "";
	private String clientName = "";
	
	public Long getId() {
		return id;
	}
	
	public void setId(Long id) {
		this.id = id;
	}
	
	public String getCommand() {
		return command;
	}
	
	public void setCommand(String command) {
		this.command = command;
	}
	
	public String getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getServer() {
		return server;
	}
	
	public void setServer(String server) {
		this.server = server;
	}
	
	public String getHostAndPort() {
		return hostAndPort;
	}
	
	public void setHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}
	
	public String getClientName() {
		return clientName;
	}
	
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	
	public Long getExecutionTime() {
		return executionTime;
	}
	
	public void setExecutionTime(Long executionTime) {
		this.executionTime = executionTime;
	}
	
	private XSlowLog(RedisObject[] properties, String server) {
		super();
		this.server = server;
		this.id = properties[0].getNumber();
		long timestamp = properties[1].getNumber();
		this.timestamp = FORMATTER.format(ofEpochMilli(timestamp * 1000).atZone(systemDefault()));
		this.executionTime = properties[2].getNumber();
		
		RedisObject[] bargs = properties[3].getArray();
		this.command = Arrays.stream(bargs).map(e -> quote(new String(codec.encode(e.getBytes())))).collect(Collectors.joining(" "));
		if (properties.length == 4) return;
		
		this.hostAndPort = properties[4].getString();
		this.clientName = properties[5].getString();
	}
	
	private String quote(String name) {
		return new StringBuilder().append('"').append(name).append('"').toString();
	}
	
	public static List<XSlowLog> valueOf(RedisObject[] binaryLogs, String server) {
		if (binaryLogs == null) return Collections.ofList();
		List<XSlowLog> logs = new ArrayList<>(binaryLogs.length);
		for (RedisObject object : binaryLogs) {
			RedisObject[] properties = object.getArray();
			logs.add(new XSlowLog(properties, server));
		}
		return logs;
	}
	
	@Override
	public String toString() {
		return "XSlowLog{" +
				"id=" + id +
				", command='" + command + '\'' +
				", timestamp='" + timestamp + '\'' +
				", server=" + server +
				", hostAndPort=" + hostAndPort +
				", clientName='" + clientName + '\'' +
				", executionTime=" + executionTime +
				'}';
	}
}
