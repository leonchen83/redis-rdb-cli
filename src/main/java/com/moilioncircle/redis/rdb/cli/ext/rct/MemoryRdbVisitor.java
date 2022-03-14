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

package com.moilioncircle.redis.rdb.cli.ext.rct;

import static com.moilioncircle.redis.rdb.cli.glossary.DataType.parse;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_BIG_KEY;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_DB_EXPIRES;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_DB_NUMBERS;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_MEASUREMENTS;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_TOTAL_MEMORY;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_TYPE_COUNT;
import static com.moilioncircle.redis.rdb.cli.glossary.Measures.MEMORY_TYPE_MEMORY;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.moilioncircle.redis.replicator.Constants.QUICKLIST_NODE_CONTAINER_PLAIN;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.STREAM_ITEM_FLAG_DELETED;
import static com.moilioncircle.redis.replicator.Constants.STREAM_ITEM_FLAG_SAMEFIELDS;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.MS;
import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.NONE;
import static java.lang.System.currentTimeMillis;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.escape.JsonEscaper;
import com.moilioncircle.redis.rdb.cli.ext.rct.support.MemoryCalculator;
import com.moilioncircle.redis.rdb.cli.ext.rct.support.MemoryMisc;
import com.moilioncircle.redis.rdb.cli.ext.rct.support.MemoryRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.rct.support.XTuple2;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.FileType;
import com.moilioncircle.redis.rdb.cli.monitor.Monitor;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorFactory;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.rdb.cli.util.MaximHeap;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public class MemoryRdbVisitor extends AbstractRctRdbVisitor implements Consumer<XTuple2>, EventListener {
	
	private static final Monitor MONITOR = MonitorFactory.getMonitor("memory");
	
	private final long bytes;
	private MemoryCalculator calc;
	private MonitorManager manager;
	private final MaximHeap<XTuple2> heap;
	private Escaper jsonEscaper = new JsonEscaper();
	
	//
	private long totalMemory = 0;
	private boolean rdb6 = true;
	private Map<Long, Tuple2<Long, Long>> dbInfo = new LinkedHashMap<>();
	
	//noinspection ThisEscapedInObjectConstruction
	public MemoryRdbVisitor(Replicator replicator, Configure configure, Args.RctArgs args, Escaper escaper) {
		super(replicator, configure, args, escaper);
		this.bytes = args.bytes;
		this.manager = new MonitorManager(configure);
		this.manager.open(MEMORY_MEASUREMENTS);
		this.heap = new MaximHeap<>(args.largest);
		this.heap.setConsumer(this);
		this.replicator.addEventListener(this);
	}
	
	private void exportJsonl(XTuple2 tuple) {
		DummyKeyValuePair kv = tuple.getV2();
		Outputs.write('{', out);
		emitString("key".getBytes());
		Outputs.write(':', out);
		Outputs.write('"', out);
		jsonEscaper.encode(kv.getKey(), out);
		Outputs.write('"', out);
		Outputs.write(',', out);
		emitField("type", parse(kv.getValueRdbType()).getValue().getBytes());
		Outputs.write(',', out);
		emitField("number_elements", kv.getLength());
		Outputs.write(',', out);
		emitField("database", kv.getDb().getDbNumber());
		Outputs.write(',', out);
		emitField("used_memory", MemoryMisc.prettySize(tuple.getV1(), configure));
		if (kv.getExpiredType() != NONE) {
			if (kv.getExpiredType() == MS) {
				Outputs.write(',', out);
				emitField("expiry", MemoryMisc.prettyDate(kv.getExpiredValue(), configure));
			} else {
				Outputs.write(',', out);
				emitField("expiry", MemoryMisc.prettyDate(kv.getExpiredValue() * 1000, configure));
			}
		}
		Outputs.write('}', out);
		Outputs.write('\n', out);
	}
	
	private void exportCsvHeader() {
		Outputs.write("database".getBytes(), out);
		delimiter(out);
		Outputs.write("type".getBytes(), out);
		delimiter(out);
		Outputs.write("key".getBytes(), out);
		delimiter(out);
		Outputs.write("size_in_bytes".getBytes(), out);
		delimiter(out);
		Outputs.write("encoding".getBytes(), out);
		delimiter(out);
		Outputs.write("num_elements".getBytes(), out);
		delimiter(out);
		Outputs.write("len_largest_element".getBytes(), out);
		delimiter(out);
		Outputs.write("expiry".getBytes(), out);
		Outputs.write('\n', out);
	}
	private void exportCsvLine(XTuple2 tuple) {
		DummyKeyValuePair kv = tuple.getV2();
		Outputs.write(String.valueOf(kv.getDb().getDbNumber()).getBytes(), out);
		delimiter(out);
		Outputs.write(parse(kv.getValueRdbType()).getValue().getBytes(), out);
		delimiter(out);
		quote(kv.getKey(), out);
		delimiter(out);
		quote(MemoryMisc.prettySize(tuple.getV1(), configure).getBytes(), out, false);
		delimiter(out);
		Outputs.write(DataType.type(kv.getValueRdbType()).getBytes(), out);
		delimiter(out);
		Outputs.write(String.valueOf(kv.getLength()).getBytes(), out);
		delimiter(out);
		quote(MemoryMisc.prettySize(kv.getMax(), configure).getBytes(), out, false);
		delimiter(out);
		if (kv.getExpiredType() != NONE) {
			if (kv.getExpiredType() == MS) {
				quote(MemoryMisc.prettyDate(kv.getExpiredValue(), configure).getBytes(), out, false);
			} else {
				quote(MemoryMisc.prettyDate(kv.getExpiredValue() * 1000, configure).getBytes(), out, false);
			}
		} else {
			quote("".getBytes(), out, false);
		}
		Outputs.write('\n', out);
	}
	
	@Override
	public void accept(XTuple2 tuple) {
		if (configure.getExportFileFormat() == FileType.CSV) {
			exportCsvLine(tuple);
		} else if (configure.getExportFileFormat() == FileType.JSONL) {
			exportJsonl(tuple);
		}
	}
	
	@Override
	public void onEvent(Replicator replicator, Event event) {
		if (event instanceof DummyKeyValuePair) {
			DummyKeyValuePair dkv = (DummyKeyValuePair) event;
			
			if (rdb6) {
				//
				totalMemory += dkv.getValue();
				//
				long expire = 0L;
				final Long expiry = dkv.getExpiredValue();
				final long dbnum = dkv.getDb() == null ? 0 : dkv.getDb().getDbNumber();
				if (dkv.getExpiredType() != null && expiry != null && expiry - currentTimeMillis() < 0) {
					expire = 1L;
				}
				
				Tuple2<Long, Long> tuple = dbInfo.get(dbnum);
				if (tuple == null) dbInfo.put(dbnum, Tuples.of(1L, expire));
				else dbInfo.put(dbnum, Tuples.of(tuple.getV1() + 1L, tuple.getV2() + expire));
			}
			
			if (!dkv.isContains() || dkv.getKey() == null) {
				return;
			}
			dkv.setValue(dkv.getValue() + calc.calcObject(dkv.getKey(), dkv.getExpiredType() != NONE));
			if (dkv.getValue() >= bytes) {
				XTuple2 tuple = new XTuple2(dkv.getValue(), dkv);
				heap.add(tuple);
			}
		} else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
			
			for (XTuple2 tuple : heap.get(true)) {
				accept(tuple);
				//
				DummyKeyValuePair kv = tuple.getV2();
				String[] properties = new String[4];
				properties[0] = new String(kv.getKey());
				properties[1] = parse(kv.getValueRdbType()).getValue();
				properties[2] = String.valueOf(kv.getDb().getDbNumber());
				properties[3] = String.valueOf(kv.getLength());
				MONITOR.set(MEMORY_BIG_KEY, properties, tuple.getV1());
			}
			
			if (rdb6) {
				MONITOR.set(MEMORY_TOTAL_MEMORY, totalMemory);
				for (Map.Entry<Long, Tuple2<Long, Long>> entry : dbInfo.entrySet()) {
					String property = String.valueOf(entry.getKey());
					MONITOR.set(MEMORY_DB_NUMBERS, property, entry.getValue().getV1());
					MONITOR.set(MEMORY_DB_EXPIRES, property, entry.getValue().getV2());
				}
			}
			
			MonitorManager.closeQuietly(manager);
		} else if (event instanceof PreRdbSyncEvent) {
			if (configure.getExportFileFormat() == FileType.CSV) {
				// csv header
				exportCsvHeader();
			}
			//
			manager.reset(MEMORY_MEASUREMENTS);
		} else if (event instanceof AuxField) {
			AuxField aux = (AuxField) event;
			if (aux.getAuxKey().equals("used-mem")) {
				MONITOR.set(MEMORY_TOTAL_MEMORY, Long.parseLong(aux.getAuxValue()));
			}
		}
	}
	
	@Override
	public int applyVersion(RedisInputStream in) throws IOException {
		int version = super.applyVersion(in);
		this.calc = new MemoryCalculator(version);
		if (version > 6) rdb6 = false;
		return version;
	}
	
	@Override
	public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
		DB db = super.applyResizeDB(in, version, context);
		String property = String.valueOf(db.getDbNumber());
		MONITOR.set(MEMORY_DB_NUMBERS, property, db.getDbsize());
		MONITOR.set(MEMORY_DB_EXPIRES, property, db.getExpires());
		return db;
	}
	
	@Override
	protected Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		byte[] val = parser.rdbLoadEncodedStringObject().first();
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(calc.calcString(val));
		kv.setContains(true);
		kv.setLength(1);
		kv.setMax(calc.calcElement(val));
		MONITOR.add(MEMORY_TYPE_COUNT, "string", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "string", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long length = len;
		long val = calc.calcListHeader();
		long max = 0;
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			max = Math.max(max, calc.calcElement(element));
			val += calc.calcString(element) + calc.calcListEntry();
			if (version < 8) val += calc.calcObjectHeader();
			len--;
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "list", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "list", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long length = len;
		long val = calc.calcHashHeader(len);
		long max = 0;
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			max = Math.max(max, calc.calcElement(element));
			val += calc.calcHashEntry() + calc.calcString(element);
			if (version < 8) val += 2 * calc.calcObjectHeader();
			len--;
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "set", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "set", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long length = 0;
		long val = calc.calcSkipListHeader(len);
		long max = 0;
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			parser.rdbLoadDoubleValue();
			max = Math.max(max, calc.calcElement(element));
			val += 8 + calc.calcString(element) + calc.calcSkipListEntry();
			if (version < 8) val += calc.calcObjectHeader();
			len--;
			length++;
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "zset", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "zset", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long length = 0;
		long val = calc.calcSkipListHeader(len);
		long max = 0;
		while (len > 0) {
			byte[] element = parser.rdbLoadEncodedStringObject().first();
			parser.rdbLoadBinaryDoubleValue();
			max = Math.max(max, calc.calcElement(element));
			val += 8 + calc.calcString(element) + calc.calcSkipListEntry();
			if (version < 8) val += calc.calcObjectHeader();
			len--;
			length++;
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "zset", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "zset", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long length = 0;
		long val = calc.calcHashHeader(len);
		long max = 0;
		while (len > 0) {
			byte[] field = parser.rdbLoadEncodedStringObject().first();
			byte[] value = parser.rdbLoadEncodedStringObject().first();
			max = Math.max(max, calc.calcElement(field));
			max = Math.max(max, calc.calcElement(value));
			val += calc.calcString(field) + calc.calcString(value) + calc.calcHashEntry();
			if (version < 8) val += 2 * calc.calcObjectHeader();
			len--;
			length++;
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "hash", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "hash", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream stream = new RedisInputStream(ary);
		long max = 0;
		long length = 0;
		BaseRdbParser.LenHelper.zmlen(stream); // zmlen
		while (true) {
			int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
			if (zmEleLen == 255) {
				DummyKeyValuePair kv = new DummyKeyValuePair();
				kv.setValueRdbType(type);
				kv.setKey(key);
				kv.setValue(ary.length());
				kv.setContains(true);
				kv.setLength(length);
				kv.setMax(max);
				MONITOR.add(MEMORY_TYPE_COUNT, "hash", 1, System.nanoTime() - mark);
				MONITOR.add(MEMORY_TYPE_MEMORY, "hash", kv.getValue());
				return context.valueOf(kv);
			}
			byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
			zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
			if (zmEleLen == 255) {
				length++;
				DummyKeyValuePair kv = new DummyKeyValuePair();
				kv.setValueRdbType(type);
				kv.setKey(key);
				kv.setValue(ary.length());
				kv.setContains(true);
				kv.setLength(length);
				kv.setMax(max);
				MONITOR.add(MEMORY_TYPE_COUNT, "hash", 1, System.nanoTime() - mark);
				MONITOR.add(MEMORY_TYPE_MEMORY, "hash", kv.getValue());
				return context.valueOf(kv);
			}
			int free = BaseRdbParser.LenHelper.free(stream);
			byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
			BaseRdbParser.StringHelper.skip(stream, free);
			max = Math.max(max, calc.calcElement(field));
			max = Math.max(max, calc.calcElement(value));
			length++;
		}
	}
	
	@Override
	protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream stream = new RedisInputStream(ary);
		long max = 0;
		BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
		BaseRdbParser.LenHelper.zltail(stream); // zltail
		int length = BaseRdbParser.LenHelper.zllen(stream);
		for (int i = 0; i < length; i++) {
			byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
			max = Math.max(max, calc.calcElement(e));
		}
		int zlend = BaseRdbParser.LenHelper.zlend(stream);
		if (zlend != 255) {
			throw new AssertionError("zlend expect 255 but " + zlend);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(ary.length());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "list", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "list", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream stream = new RedisInputStream(ary);
		long max = 0;
		int encoding = BaseRdbParser.LenHelper.encoding(stream);
		long length = BaseRdbParser.LenHelper.lenOfContent(stream);
		for (long i = 0; i < length; i++) {
			String element;
			switch (encoding) {
				case 2:
					element = String.valueOf(stream.readInt(2));
					break;
				case 4:
					element = String.valueOf(stream.readInt(4));
					break;
				case 8:
					element = String.valueOf(stream.readLong(8));
					break;
				default:
					throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
			}
			max = Math.max(max, calc.calcElement(element.getBytes()));
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(ary.length());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "set", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "set", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream stream = new RedisInputStream(ary);
		long max = 0;
		long length = 0;
		BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
		BaseRdbParser.LenHelper.zltail(stream); // zltail
		int zllen = BaseRdbParser.LenHelper.zllen(stream);
		while (zllen > 0) {
			byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			max = Math.max(max, calc.calcElement(element));
			length++;
		}
		int zlend = BaseRdbParser.LenHelper.zlend(stream);
		if (zlend != 255) {
			throw new AssertionError("zlend expect 255 but " + zlend);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(ary.length());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "zset", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "zset", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream listPack = new RedisInputStream(ary);
		long max = 0;
		long length = 0;
		listPack.skip(4); // total-bytes
		int len = listPack.readInt(2);
		while (len > 0) {
			byte[] element = listPackEntry(listPack);
			len--;
			Double.valueOf(Strings.toString(listPackEntry(listPack)));
			len--;
			max = Math.max(max, calc.calcElement(element));
			length++;
		}
		int lpend = listPack.read(); // lp-end
		if (lpend != 255) {
			throw new AssertionError("listpack expect 255 but " + lpend);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(ary.length());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "zset", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "zset", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream stream = new RedisInputStream(ary);
		long max = 0;
		long length = 0;
		BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
		BaseRdbParser.LenHelper.zltail(stream); // zltail
		int zllen = BaseRdbParser.LenHelper.zllen(stream);
		while (zllen > 0) {
			byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
			zllen--;
			max = Math.max(max, calc.calcElement(field));
			max = Math.max(max, calc.calcElement(value));
			length++;
		}
		int zlend = BaseRdbParser.LenHelper.zlend(stream);
		if (zlend != 255) {
			throw new AssertionError("zlend expect 255 but " + zlend);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(ary.length());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "hash", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "hash", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		ByteArray ary = parser.rdbLoadPlainStringObject();
		RedisInputStream listPack = new RedisInputStream(ary);
		long max = 0;
		long length = 0;
		listPack.skip(4); // total-bytes
		int len = listPack.readInt(2);
		while (len > 0) {
			byte[] field = listPackEntry(listPack);
			len--;
			byte[] value = listPackEntry(listPack);
			len--;
			max = Math.max(max, calc.calcElement(field));
			max = Math.max(max, calc.calcElement(value));
			length++;
		}
		int lpend = listPack.read(); // lp-end
		if (lpend != 255) {
			throw new AssertionError("listpack expect 255 but " + lpend);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(ary.length());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "hash", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "hash", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long val = 0;
		long max = 0;
		long length = 0;
		for (long i = 0; i < len; i++) {
			ByteArray ary = parser.rdbGenericLoadStringObject(RDB_LOAD_NONE);
			RedisInputStream stream = new RedisInputStream(ary);
			val += ary.length();
			BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
			BaseRdbParser.LenHelper.zltail(stream); // zltail
			int zllen = BaseRdbParser.LenHelper.zllen(stream);
			for (int j = 0; j < zllen; j++) {
				byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
				max = Math.max(max, calc.calcElement(e));
				length++;
			}
			int zlend = BaseRdbParser.LenHelper.zlend(stream);
			if (zlend != 255) {
				throw new AssertionError("zlend expect 255 but " + zlend);
			}
		}
		val += calc.calcQuickListHeader(len);
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "list", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "list", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		BaseRdbParser parser = new BaseRdbParser(in);
		long len = parser.rdbLoadLen().len;
		long val = 0;
		long max = 0;
		long length = 0;
		for (long i = 0; i < len; i++) {
			long container = parser.rdbLoadLen().len;
			ByteArray bytes = parser.rdbLoadPlainStringObject();
			val += bytes.length();
			if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
				max = Math.max(max, calc.calcElement(bytes.first()));
				length++;
			} else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
				RedisInputStream listPack = new RedisInputStream(bytes);
				listPack.skip(4); // total-bytes
				int innerLen = listPack.readInt(2);
				for (int j = 0; j < innerLen; j++) {
					byte[] e = listPackEntry(listPack);
					max = Math.max(max, calc.calcElement(e));
					length++;
				}
				int lpend = listPack.read(); // lp-end
				if (lpend != 255) {
					throw new AssertionError("listpack expect 255 but " + lpend);
				}
			} else {
				throw new UnsupportedOperationException(String.valueOf(container));
			}
		}
		val += calc.calcQuickListHeader(len);
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(val);
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "list", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "list", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		MemoryRawByteListener listener = new MemoryRawByteListener();
		replicator.addRawByteListener(listener);
		try {
			super.doApplyModule(in, version, key, type, context);
		} finally {
			replicator.removeRawByteListener(listener);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(listener.getLength());
		kv.setContains(true);
		kv.setLength(1);
		kv.setMax(listener.getLength());
		MONITOR.add(MEMORY_TYPE_COUNT, "module", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "module", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		MemoryRawByteListener listener = new MemoryRawByteListener();
		replicator.addRawByteListener(listener);
		try {
			super.doApplyModule2(in, version, key, type, context);
		} finally {
			replicator.removeRawByteListener(listener);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(listener.getLength());
		kv.setContains(true);
		kv.setLength(1);
		kv.setMax(listener.getLength());
		MONITOR.add(MEMORY_TYPE_COUNT, "module", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "module", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		long length = 0;
		long max = 0;
		MemoryRawByteListener listener = new MemoryRawByteListener();
		replicator.addRawByteListener(listener);
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			long listPacks = parser.rdbLoadLen().len;
			while (listPacks-- > 0) {
				parser.rdbLoadPlainStringObject();
				RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
				listPack.skip(4);
				listPack.skip(2);
				long count = Long.parseLong(Strings.toString(listPackEntry(listPack))); // count
				long deleted = Long.parseLong(Strings.toString(listPackEntry(listPack))); // deleted
				int numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack))); // num-fields
				byte[][] tempFields = new byte[numFields][];
				for (int i = 0; i < numFields; i++) {
					tempFields[i] = listPackEntry(listPack);
				}
				listPackEntry(listPack); // 0
				
				long total = count + deleted;
				while (total-- > 0) {
					int flag = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
					listPackEntry(listPack);
					listPackEntry(listPack);
					boolean delete = (flag & STREAM_ITEM_FLAG_DELETED) != 0;
					if ((flag & STREAM_ITEM_FLAG_SAMEFIELDS) != 0) {
						for (int i = 0; i < numFields; i++) {
							byte[] value = listPackEntry(listPack);
							byte[] field = tempFields[i];
							max = Math.max(max, calc.calcElement(value));
							max = Math.max(max, calc.calcElement(field));
							if (!delete) length++;
						}
					} else {
						numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
						for (int i = 0; i < numFields; i++) {
							byte[] field = listPackEntry(listPack);
							byte[] value = listPackEntry(listPack);
							max = Math.max(max, calc.calcElement(value));
							max = Math.max(max, calc.calcElement(field));
							if (!delete) length++;
						}
					}
					listPackEntry(listPack); // lp-count
				}
				int lpend = listPack.read(); // lp-end
				if (lpend != 255) {
					throw new AssertionError("listpack expect 255 but " + lpend);
				}
			}
			SkipRdbParser skip = new SkipRdbParser(in);
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			long groupCount = skip.rdbLoadLen().len;
			while (groupCount-- > 0) {
				skip.rdbLoadPlainStringObject();
				skip.rdbLoadLen();
				skip.rdbLoadLen();
				long groupPel = skip.rdbLoadLen().len;
				while (groupPel-- > 0) {
					in.skip(16);
					skip.rdbLoadMillisecondTime();
					skip.rdbLoadLen();
				}
				long consumerCount = skip.rdbLoadLen().len;
				while (consumerCount-- > 0) {
					skip.rdbLoadPlainStringObject();
					skip.rdbLoadMillisecondTime();
					long consumerPel = skip.rdbLoadLen().len;
					while (consumerPel-- > 0) {
						in.skip(16);
					}
				}
			}
		} finally {
			replicator.removeRawByteListener(listener);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(listener.getLength());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "stream", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "stream", kv.getValue());
		return context.valueOf(kv);
	}
	
	@Override
	protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
		long mark = System.nanoTime();
		long length = 0;
		long max = 0;
		MemoryRawByteListener listener = new MemoryRawByteListener();
		replicator.addRawByteListener(listener);
		try {
			BaseRdbParser parser = new BaseRdbParser(in);
			long listPacks = parser.rdbLoadLen().len;
			while (listPacks-- > 0) {
				parser.rdbLoadPlainStringObject();
				RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
				listPack.skip(4);
				listPack.skip(2);
				long count = Long.parseLong(Strings.toString(listPackEntry(listPack))); // count
				long deleted = Long.parseLong(Strings.toString(listPackEntry(listPack))); // deleted
				int numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack))); // num-fields
				byte[][] tempFields = new byte[numFields][];
				for (int i = 0; i < numFields; i++) {
					tempFields[i] = listPackEntry(listPack);
				}
				listPackEntry(listPack); // 0
				
				long total = count + deleted;
				while (total-- > 0) {
					int flag = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
					listPackEntry(listPack);
					listPackEntry(listPack);
					boolean delete = (flag & STREAM_ITEM_FLAG_DELETED) != 0;
					if ((flag & STREAM_ITEM_FLAG_SAMEFIELDS) != 0) {
						for (int i = 0; i < numFields; i++) {
							byte[] value = listPackEntry(listPack);
							byte[] field = tempFields[i];
							max = Math.max(max, calc.calcElement(value));
							max = Math.max(max, calc.calcElement(field));
							if (!delete) length++;
						}
					} else {
						numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
						for (int i = 0; i < numFields; i++) {
							byte[] field = listPackEntry(listPack);
							byte[] value = listPackEntry(listPack);
							max = Math.max(max, calc.calcElement(value));
							max = Math.max(max, calc.calcElement(field));
							if (!delete) length++;
						}
					}
					listPackEntry(listPack); // lp-count
				}
				int lpend = listPack.read(); // lp-end
				if (lpend != 255) {
					throw new AssertionError("listpack expect 255 but " + lpend);
				}
			}
			SkipRdbParser skip = new SkipRdbParser(in);
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			skip.rdbLoadLen();
			long groupCount = skip.rdbLoadLen().len;
			while (groupCount-- > 0) {
				skip.rdbLoadPlainStringObject();
				skip.rdbLoadLen();
				skip.rdbLoadLen();
				skip.rdbLoadLen();
				long groupPel = skip.rdbLoadLen().len;
				while (groupPel-- > 0) {
					in.skip(16);
					skip.rdbLoadMillisecondTime();
					skip.rdbLoadLen();
				}
				long consumerCount = skip.rdbLoadLen().len;
				while (consumerCount-- > 0) {
					skip.rdbLoadPlainStringObject();
					skip.rdbLoadMillisecondTime();
					long consumerPel = skip.rdbLoadLen().len;
					while (consumerPel-- > 0) {
						in.skip(16);
					}
				}
			}
		} finally {
			replicator.removeRawByteListener(listener);
		}
		DummyKeyValuePair kv = new DummyKeyValuePair();
		kv.setValueRdbType(type);
		kv.setKey(key);
		kv.setValue(listener.getLength());
		kv.setContains(true);
		kv.setLength(length);
		kv.setMax(max);
		MONITOR.add(MEMORY_TYPE_COUNT, "stream", 1, System.nanoTime() - mark);
		MONITOR.add(MEMORY_TYPE_MEMORY, "stream", kv.getValue());
		return context.valueOf(kv);
	}
}