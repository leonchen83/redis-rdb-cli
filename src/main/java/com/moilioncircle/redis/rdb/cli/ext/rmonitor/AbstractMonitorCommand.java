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

import java.util.concurrent.TimeUnit;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractMonitorCommand implements MonitorCommand {
	
	protected static final Monitor monitor = MonitorFactory.getMonitor("monitor");
	
	protected String name;
	protected Configure configure;
	protected MonitorManager manager;
	
	public AbstractMonitorCommand(String name, Configure configure) {
		this.name = name;
		this.configure = configure;
		this.manager = new MonitorManager(configure);
		this.manager.open();
	}
	
	@Override
	public void close() {
		MonitorManager.closeQuietly(manager);
	}
	
	protected void delay(long time, TimeUnit unit) {
		try {
			unit.sleep(time);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
