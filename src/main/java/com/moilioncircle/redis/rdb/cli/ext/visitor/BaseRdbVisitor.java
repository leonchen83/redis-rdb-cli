/*
 * Copyright 2018-2019 Baoyi Chen
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

package com.moilioncircle.redis.rdb.cli.ext.visitor;

import static com.moilioncircle.redis.rdb.cli.glossary.Guard.DRAIN;
import static com.moilioncircle.redis.rdb.cli.glossary.Guard.PASS;
import static com.moilioncircle.redis.rdb.cli.glossary.Guard.SAVE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_LISTPACK;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPMAP;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET_INTSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET_LISTPACK;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS_3;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STRING;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_LISTPACK;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_ZIPLIST;

import java.io.IOException;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.GuardRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.RdbValueVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbValueVisitor;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * @author Baoyi Chen
 */
public abstract class BaseRdbVisitor extends DefaultRdbVisitor {
	
	protected Filter filter;
	protected Configure configure;
	protected RdbValueVisitor valueVisitor;
	protected GuardRawByteListener listener;
	
	public BaseRdbVisitor(Replicator replicator, Configure configure, Filter filter) {
		super(replicator);
		this.filter = filter;
		this.configure = configure;
		this.valueVisitor = new SkipRdbValueVisitor(replicator);
	}
	
	protected int getVersion(int version) {
		if (configure.getDumpRdbVersion() == -1) {
			return version;
		} else {
			return configure.getDumpRdbVersion();
		}
	}
	
	@Override
	public Event applyString(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_STRING, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyString(in, version, key, RDB_TYPE_STRING, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyString(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_LIST, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyList(in, version, key, RDB_TYPE_LIST, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyList(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applySet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_SET, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplySet(in, version, key, RDB_TYPE_SET, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applySet(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applySetListPack(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_SET_LISTPACK, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplySetListPack(in, version, key, RDB_TYPE_SET_LISTPACK, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applySetListPack(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyZSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyZSet(in, version, key, RDB_TYPE_ZSET, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyZSet(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyZSet2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET_2, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyZSet2(in, version, key, RDB_TYPE_ZSET_2, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyZSet2(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyHash(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_HASH, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyHash(in, version, key, RDB_TYPE_HASH, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyHash(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyHashZipMap(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_HASH_ZIPMAP, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyHashZipMap(in, version, key, RDB_TYPE_HASH_ZIPMAP, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyHashZipMap(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyListZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_LIST_ZIPLIST, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyListZipList(in, version, key, RDB_TYPE_LIST_ZIPLIST, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyListZipList(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applySetIntSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_SET_INTSET, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplySetIntSet(in, version, key, RDB_TYPE_SET_INTSET, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applySetIntSet(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyZSetZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET_ZIPLIST, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyZSetZipList(in, version, key, RDB_TYPE_ZSET_ZIPLIST, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyZSetZipList(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyZSetListPack(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_ZSET_LISTPACK, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyZSetListPack(in, version, key, RDB_TYPE_ZSET_LISTPACK, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyZSetListPack(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyHashZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_HASH_ZIPLIST, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyHashZipList(in, version, key, RDB_TYPE_HASH_ZIPLIST, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyHashZipList(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyHashListPack(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_HASH_LISTPACK, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyHashListPack(in, version, key, RDB_TYPE_HASH_LISTPACK, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyHashListPack(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyListQuickList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_LIST_QUICKLIST, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyListQuickList(in, version, key, RDB_TYPE_LIST_QUICKLIST, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyListQuickList(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyListQuickList2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_LIST_QUICKLIST_2, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyListQuickList2(in, version, key, RDB_TYPE_LIST_QUICKLIST_2, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyListQuickList2(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyModule(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_MODULE, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyModule(in, version, key, RDB_TYPE_MODULE, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyModule(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyModule2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_MODULE_2, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyModule2(in, version, key, RDB_TYPE_MODULE_2, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyModule2(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyStreamListPacks(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_STREAM_LISTPACKS, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyStreamListPacks(in, version, key, RDB_TYPE_STREAM_LISTPACKS, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyStreamListPacks(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyStreamListPacks2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_STREAM_LISTPACKS_2, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyStreamListPacks2(in, version, key, RDB_TYPE_STREAM_LISTPACKS_2, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyStreamListPacks2(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	@Override
	public Event applyStreamListPacks3(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			byte[] key = parser.rdbLoadEncodedStringObject().first();
			boolean contains = filter.contains(context.getDb().getDbNumber(), RDB_TYPE_STREAM_LISTPACKS_3, Strings.toString(key));
			if (contains) {
				if (listener != null) listener.setGuard(DRAIN);
				return doApplyStreamListPacks3(in, version, key, RDB_TYPE_STREAM_LISTPACKS_3, context);
			} else {
				if (listener != null) listener.setGuard(PASS);
				valueVisitor.applyStreamListPacks3(in, version);
				return context.valueOf(new DummyKeyValuePair());
			}
		} finally {
			if (listener != null) listener.setGuard(SAVE);
		}
	}
	
	protected Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyString(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyList(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applySet(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplySetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applySetListPack(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyZSet(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyZSet2(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyHash(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyHashZipMap(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyListZipList(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applySetIntSet(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyZSetZipList(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyZSetListPack(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyHashZipList(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyHashListPack(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyListQuickList(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyListQuickList2(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyModule(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyModule2(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyStreamListPacks(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyStreamListPacks2(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
	
	protected Event doApplyStreamListPacks3(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		valueVisitor.applyStreamListPacks3(in, version);
		return context.valueOf(new DummyKeyValuePair());
	}
}
