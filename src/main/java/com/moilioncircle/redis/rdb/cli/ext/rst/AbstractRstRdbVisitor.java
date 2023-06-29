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

package com.moilioncircle.redis.rdb.cli.ext.rst;

import static com.moilioncircle.redis.rdb.cli.glossary.Measures.ENDPOINT_MEASUREMENTS;

import java.io.IOException;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.escape.RawEscaper;
import com.moilioncircle.redis.rdb.cli.ext.visitor.BaseRdbVisitor;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.DumpRdbValueVisitor;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpFunction;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractRstRdbVisitor extends BaseRdbVisitor {
	
	protected final boolean flush;
	protected final boolean replace;
	protected final MonitorManager manager;
	protected final Escaper raw = new RawEscaper();
	protected final DumpRdbValueVisitor valueVisitor;
	
	public AbstractRstRdbVisitor(Replicator replicator, Configure configure, Filter filter, boolean replace) {
		super(replicator, configure, filter);
		this.replace = replace;
		this.flush = configure.isMigrateFlush();
		this.manager = new MonitorManager(configure);
		this.manager.open(ENDPOINT_MEASUREMENTS);
		this.valueVisitor = new DumpRdbValueVisitor(replicator, configure.getDumpRdbVersion(), configure.getOutputBufferSize());
	}
	
	@Override
	public Event applyFunction(RedisInputStream in, int version) throws IOException {
		DumpFunction function = valueVisitor.applyFunction(in, version);
		return function;
	}
	
	@Override
	public Event applyFunction2(RedisInputStream in, int version) throws IOException {
		DumpFunction function = valueVisitor.applyFunction2(in, version);
		return function;
	}
	
	@Override
	protected Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyString(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyList(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applySet(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplySetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applySetListPack(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyZSet(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyZSet2(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyHash(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyHashZipMap(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyListZipList(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applySetIntSet(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyZSetZipList(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyZSetListPack(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyHashZipList(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyHashListPack(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyListQuickList(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyListQuickList2(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyModule(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyModule2(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyStreamListPacks(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyStreamListPacks2(in, version));
		return context.valueOf(dump);
	}
	
	@Override
	protected Event doApplyStreamListPacks3(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		DumpKeyValuePair dump = new DumpKeyValuePair();
		dump.setKey(key);
		dump.setValue(valueVisitor.applyStreamListPacks3(in, version));
		return context.valueOf(dump);
	}
}
