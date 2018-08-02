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

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.Escape;
import com.moilioncircle.redis.rdb.cli.metric.MetricReporterFactory;
import com.moilioncircle.redis.rdb.cli.util.CmpHeap;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.rdb.cli.util.type.Tuple2;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;
import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.ScheduledReporter;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.moilioncircle.redis.rdb.cli.ext.rct.MemRdbVisitor.Tuple2Ex;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.STREAM_ITEM_FLAG_DELETED;
import static com.moilioncircle.redis.replicator.Constants.STREAM_ITEM_FLAG_SAMEFIELDS;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;
import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.NONE;
import static io.dropwizard.metrics5.MetricName.build;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;

/**
 * @author Baoyi Chen
 */
public class MemRdbVisitor extends AbstractRdbVisitor implements Consumer<Tuple2Ex>, EventListener {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final Long bytes;
    private MemCalculator size;
    private ScheduledReporter reporter;
    private final CmpHeap<Tuple2Ex> heap;
    private final CmpHeap<Tuple2Ex> metricHeap;
    private MetricRegistry registry = new MetricRegistry();

    private Counter counterSet = registry.counter("count_type_set");
    private Counter counterList = registry.counter("count_type_list");
    private Counter counterZSet = registry.counter("count_type_zset");
    private Counter counterHash = registry.counter("count_type_hash");
    private Counter counterString = registry.counter("count_type_string");
    private Counter counterModule = registry.counter("count_type_module");
    private Counter counterStream = registry.counter("count_type_stream");

    private Counter counterSetMem = registry.counter("mem_total_type_set");
    private Counter counterListMem = registry.counter("mem_total_type_list");
    private Counter counterZSetMem = registry.counter("mem_total_type_zset");
    private Counter counterHashMem = registry.counter("mem_total_type_hash");
    private Counter counterStringMem = registry.counter("mem_total_type_string");
    private Counter counterModuleMem = registry.counter("mem_total_type_module");
    private Counter counterStreamMem = registry.counter("mem_total_type_stream");

    private Histogram histogram = registry.histogram("mem_usage_histogram");

    public MemRdbVisitor(Replicator replicator, Configure configure, File out, List<Long> db, List<String> regexs, List<DataType> types, Escape escape, Long largest, Long bytes) {
        super(replicator, configure, out, db, regexs, types, escape);
        this.bytes = bytes;
        this.heap = new CmpHeap<>(largest == null ? -1 : largest.intValue());
        this.metricHeap = new CmpHeap<>(largest == null ? 100 : largest.intValue());
        this.heap.setConsumer(this);
        this.replicator.addEventListener(this);
    }

    @Override
    public void accept(Tuple2Ex tuple) {
        DummyKeyValuePair kv = tuple.getV2();
        OutputStreams.write(String.valueOf(kv.getDb().getDbNumber()).getBytes(), out);
        delimiter(out);
        OutputStreams.write(DataType.parse(kv.getValueRdbType()).getValue().getBytes(), out);
        delimiter(out);
        quote(kv.getKey(), out);
        delimiter(out);
        quote(com.moilioncircle.redis.rdb.cli.util.Strings.pretty(tuple.getV1()).getBytes(), out, false);
        delimiter(out);
        OutputStreams.write(DataType.type(kv.getValueRdbType()).getBytes(), out);
        delimiter(out);
        OutputStreams.write(String.valueOf(kv.getLength()).getBytes(), out);
        delimiter(out);
        quote(com.moilioncircle.redis.rdb.cli.util.Strings.pretty(kv.getMax()).getBytes(), out, false);
        delimiter(out);
        if (kv.getExpiredType() != NONE) {
            quote(FORMATTER.format(ofEpochMilli(kv.getExpiredValue()).atZone(systemDefault())).getBytes(), out, false);
        } else {
            quote("".getBytes(), out, false);
        }
        OutputStreams.write('\n', out);
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof DummyKeyValuePair) {
            DummyKeyValuePair dkv = (DummyKeyValuePair) event;
            if (!dkv.isContains() || dkv.getKey() == null) return;
            dkv.setValue(dkv.getValue() + size.object(dkv.getKey(), dkv.getExpiredType() != NONE));
            if (bytes == null || dkv.getValue() >= bytes) {
                Tuple2Ex tuple = new Tuple2Ex(dkv.getValue(), dkv);
                heap.add(tuple);
                metricHeap.add(tuple);
            }
            histogram.update(dkv.getValue());
        } else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            for (Tuple2Ex tuple : heap.get(true)) {
                accept(tuple);
            }
            for (Tuple2Ex tuple : metricHeap.get(true)) {
                String key = Strings.toString(tuple.getV2().getKey());
                registry.gauge(build("key:" + key), () -> tuple::getV1);
            }
            if (this.reporter != null) {
                this.reporter.report();
                this.reporter.close();
            }
        } else if (event instanceof PreRdbSyncEvent) {
            // header
            // database,type,key,size_in_bytes,encoding,num_elements,len_largest_element
            OutputStreams.write("database".getBytes(), out);
            delimiter(out);
            OutputStreams.write("type".getBytes(), out);
            delimiter(out);
            OutputStreams.write("key".getBytes(), out);
            delimiter(out);
            OutputStreams.write("size_in_bytes".getBytes(), out);
            delimiter(out);
            OutputStreams.write("encoding".getBytes(), out);
            delimiter(out);
            OutputStreams.write("num_elements".getBytes(), out);
            delimiter(out);
            OutputStreams.write("len_largest_element".getBytes(), out);
            delimiter(out);
            OutputStreams.write("expiry".getBytes(), out);
            OutputStreams.write('\n', out);
            if (this.reporter != null) this.reporter.close();
            this.reporter = MetricReporterFactory.create(configure, registry, "redis_rdb_cli_mem");
            this.reporter.start(5, TimeUnit.SECONDS);
        } else if (event instanceof AuxField) {
            AuxField aux = (AuxField) event;
            if (aux.getAuxKey().equals("used-mem")) {
                registry.gauge(build("mem_total_type_all"), () -> () -> Long.parseLong(aux.getAuxValue()));
            }
        }
    }

    @Override
    public int applyVersion(RedisInputStream in) throws IOException {
        int version = super.applyVersion(in);
        this.size = new MemCalculator(version);
        return version;
    }

    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(size.string(val));
        kv.setContains(contains);
        kv.setLength(1);
        kv.setMax(size.element(val));
        counterString.inc();
        counterStringMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        long length = len;
        long val = size.linkedlist();
        long max = 0;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            max = Math.max(max, size.element(element));
            val += size.string(element) + size.linkedlistEntry();
            if (version < 8) val += size.robj();
            len--;
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(val);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterList.inc();
        counterListMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        long length = len;
        long val = size.hash(len);
        long max = 0;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            max = Math.max(max, size.element(element));
            val += size.hashEntry() + size.string(element);
            if (version < 8) val += 2 * size.robj();
            len--;
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(val);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterSet.inc();
        counterSetMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        long length = 0;
        long val = size.skiplist(len);
        long max = 0;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            parser.rdbLoadDoubleValue();
            max = Math.max(max, size.element(element));
            val += 8 + size.string(element) + size.skiplistEntry();
            if (version < 8) val += size.robj();
            len--;
            length++;
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(val);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterZSet.inc();
        counterZSetMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        long length = 0;
        long val = size.skiplist(len);
        long max = 0;
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            parser.rdbLoadBinaryDoubleValue();
            max = Math.max(max, size.element(element));
            val += 8 + size.string(element) + size.skiplistEntry();
            if (version < 8) val += size.robj();
            len--;
            length++;
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(val);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterZSet.inc();
        counterZSetMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        long length = 0;
        long val = size.hash(len);
        long max = 0;
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            max = Math.max(max, size.element(field));
            max = Math.max(max, size.element(value));
            val += size.string(field) + size.string(value) + size.hashEntry();
            if (version < 8) val += 2 * size.robj();
            len--;
            length++;
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(val);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterHash.inc();
        counterHashMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
                kv.setContains(contains);
                kv.setLength(length);
                kv.setMax(max);
                counterHash.inc();
                counterHashMem.inc(kv.getValue());
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
                kv.setContains(contains);
                kv.setLength(length);
                kv.setMax(max);
                counterHash.inc();
                counterHashMem.inc(kv.getValue());
                return context.valueOf(kv);
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            max = Math.max(max, size.element(field));
            max = Math.max(max, size.element(value));
            length++;
        }
    }

    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        ByteArray ary = parser.rdbLoadPlainStringObject();
        RedisInputStream stream = new RedisInputStream(ary);
        long max = 0;
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int length = BaseRdbParser.LenHelper.zllen(stream);
        for (int i = 0; i < length; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            max = Math.max(max, size.element(e));
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(ary.length());
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterList.inc();
        counterListMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
            max = Math.max(max, size.element(element.getBytes()));
        }
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(ary.length());
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterSet.inc();
        counterSetMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
            max = Math.max(max, size.element(element));
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
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterZSet.inc();
        counterZSetMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
            max = Math.max(max, size.element(field));
            max = Math.max(max, size.element(value));
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
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterHash.inc();
        counterHashMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
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
                max = Math.max(max, size.element(e));
                length++;
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        val += size.quicklist(len);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(val);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterList.inc();
        counterListMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        LengthRawByteListener listener = new LengthRawByteListener();
        replicator.addRawByteListener(listener);
        super.doApplyModule(in, version, key, contains, type, context);
        replicator.removeRawByteListener(listener);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.length);
        kv.setContains(contains);
        kv.setLength(1);
        kv.setMax(listener.length);
        counterModule.inc();
        counterModuleMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        LengthRawByteListener listener = new LengthRawByteListener();
        replicator.addRawByteListener(listener);
        super.doApplyModule2(in, version, key, contains, type, context);
        replicator.removeRawByteListener(listener);
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.length);
        kv.setContains(contains);
        kv.setLength(1);
        kv.setMax(listener.length);
        counterModule.inc();
        counterModuleMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        long length = 0;
        long max = 0;
        LengthRawByteListener listener = new LengthRawByteListener();
        replicator.addRawByteListener(listener);
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
                        max = Math.max(max, size.element(value));
                        max = Math.max(max, size.element(field));
                        if (!delete) length++;
                    }
                } else {
                    numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
                    for (int i = 0; i < numFields; i++) {
                        byte[] field = listPackEntry(listPack);
                        byte[] value = listPackEntry(listPack);
                        max = Math.max(max, size.element(value));
                        max = Math.max(max, size.element(field));
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
        DummyKeyValuePair kv = new DummyKeyValuePair();
        kv.setValueRdbType(type);
        kv.setKey(key);
        kv.setValue(listener.length);
        kv.setContains(contains);
        kv.setLength(length);
        kv.setMax(max);
        counterStream.inc();
        counterStreamMem.inc(kv.getValue());
        return context.valueOf(kv);
    }

    static class Tuple2Ex extends Tuple2<Long, DummyKeyValuePair> implements Comparable<Tuple2Ex> {

        public Tuple2Ex(Long v1, DummyKeyValuePair v2) {
            super(v1, v2);
        }

        @Override
        public int compareTo(Tuple2Ex that) {
            return Long.compare(this.getV1(), that.getV1());
        }
    }

    private static class LengthRawByteListener implements RawByteListener {
        private long length;

        @Override
        public void handle(byte... rawBytes) {
            length += rawBytes.length;
        }
    }

    public static class MemCalculator {

        private int version;

        public MemCalculator(int version) {
            this.version = version;
        }

        public long robj() {
            return 8 + 8;
        }

        public long string(byte[] bytes) {
            long[] num = parseLong(bytes);
            if (num[1] != -1) {
                if (num[0] < 10000) return 0;
                else return 8;
            }
            long len = bytes.length;
            if (version < 7) {
                return malloc(len + 8 + 1);
            }
            if (len < (1L << 5)) {
                return malloc(len + 1 + 1);
            } else if (len < (1L << 8)) {
                return malloc(len + 1 + 2 + 1);
            } else if (len < (1L << 16)) {
                return malloc(len + 1 + 4 + 1);
            } else if (len < (1L << 32)) {
                return malloc(len + 1 + 8 + 1);
            } else {
                return malloc(len + 1 + 16 + 1);
            }
        }

        public long object(byte[] key, boolean expiry) {
            return hashEntry() + string(key) + robj() + expiry(expiry);
        }

        public long expiry(boolean expiry) {
            if (!expiry) return 0;
            return hashEntry() + 8;
        }

        public long hash(long size) {
            return (long) (4 + 7 * 8 + 4 * 8 + power(size) * 8 * 1.5);
        }

        public long hashEntry() {
            return 2 * 8 + 8;
        }

        public long linkedlist() {
            return 8 + 5 * 8;
        }

        public long quicklist(long zip_count) {
            long quicklist = 2 * 8 + 8 + 2 * 4;
            long quickitem = 4 * 8 + 8 + 2 * 4;
            return quicklist + zip_count * quickitem;
        }

        public long linkedlistEntry() {
            return 3 * 8;
        }

        public long skiplist(long size) {
            return 2 * 8 + hash(size) + (2 * 8 + 16);
        }

        public long skiplistEntry() {
            return hashEntry() + 2 * 8 + 8 + (8 + 8) * random();
        }

        public long power(long size) {
            long p = 1;
            long tmp = p;
            while (tmp <= size) {
                tmp <<= 1;
                if (tmp <= 0) {
                    return p;
                } else {
                    p = tmp;
                }
            }
            return p;
        }

        public long random() {
            long level = 1;
            int r = ThreadLocalRandom.current().nextInt(0xFFFF);
            while (r < 0.25 * 0xFFFF) {
                level += 1;
                r = ThreadLocalRandom.current().nextInt(0xFFFF);
            }
            return Math.min(level, 32);
        }

        public long element(byte[] element) {
            if (parseLong(element)[1] != -1) {
                return 8;
            } else {
                return element.length;
            }
        }

        public long[] parseLong(byte[] array) {
            long[] err = new long[]{-1L, -1L};
            if (array == null) return err;
            long t = 0L;
            long sig = 1L;
            int i = 0;
            if (array[0] == '-') {
                sig = -1L;
                i = 1;
            } else if (array[0] == '+') {
                sig = 1L;
                i = 1;
            }
            for (; i < array.length; i++) {
                if (array[i] >= '0' && array[i] <= '9') {
                    t = t * (long) Math.pow(10, i) + array[i] - 48;
                    if (t < 0) return err;
                } else {
                    return err;
                }
            }
            return new long[]{t * sig, 1L};
        }

        public long malloc(long size) {
            int idx = Arrays.binarySearch(JEMALLOC_SIZE, size);
            if (idx < 0) idx = -idx - 1;
            return idx < JEMALLOC_SIZE.length ? JEMALLOC_SIZE[idx] : size;
        }

        public static final long[] JEMALLOC_SIZE = new long[]{
                8L, 16L, 24L, 32L, 40L, 48L, 56L, 64L, 80L, 96L, 112L, 128L, 160L, 192L, 224L, 256L, 320L, 384L, 448L,
                512L, 640L, 768L, 896L, 1024L, 1280L, 1536L, 1792L, 2048L, 2560L, 3072L, 3584L, 4096L, 5120L, 6144L,
                7168L, 8192L, 10240L, 12288L, 14336L, 16384L, 20480L, 24576L, 28672L, 32768L, 40960L, 49152L, 57344L,
                65536L, 81920L, 98304L, 114688L, 131072L, 163840L, 196608L, 229376L, 262144L, 327680L, 393216L, 458752L,
                524288L, 655360L, 786432L, 917504L, 1048576L, 1310720L, 1572864L, 1835008L, 2097152L, 2621440L, 3145728L,
                3670016L, 4194304L, 5242880L, 6291456L, 7340032L, 8388608L, 10485760L, 12582912L, 14680064L, 16777216L,
                20971520L, 25165824L, 29360128L, 33554432L, 41943040L, 50331648L, 58720256L, 67108864L, 83886080L, 100663296L,
                117440512L, 134217728L, 167772160L, 201326592L, 234881024L, 268435456L, 335544320L, 402653184L, 469762048L,
                536870912L, 671088640L, 805306368L, 939524096L, 1073741824L, 1342177280L, 1610612736L, 1879048192L, 2147483648L,
                2684354560L, 3221225472L, 3758096384L, 4294967296L, 5368709120L, 6442450944L, 7516192768L, 8589934592L,
                10737418240L, 12884901888L, 15032385536L, 17179869184L, 21474836480L, 25769803776L, 30064771072L, 34359738368L,
                42949672960L, 51539607552L, 60129542144L, 68719476736L, 85899345920L, 103079215104L, 120259084288L, 137438953472L,
                171798691840L, 206158430208L, 240518168576L, 274877906944L, 343597383680L, 412316860416L, 481036337152L,
                549755813888L, 687194767360L, 824633720832L, 962072674304L, 1099511627776L, 1374389534720L, 1649267441664L,
                1924145348608L, 2199023255552L, 2748779069440L, 3298534883328L, 3848290697216L, 4398046511104L, 5497558138880L,
                6597069766656L, 7696581394432L, 8796093022208L, 10995116277760L, 13194139533312L, 15393162788864L, 17592186044416L,
                21990232555520L, 26388279066624L, 30786325577728L, 35184372088832L, 43980465111040L, 52776558133248L,
                61572651155456L, 70368744177664L, 87960930222080L, 105553116266496L, 123145302310912L, 140737488355328L,
                175921860444160L, 211106232532992L, 246290604621824L, 281474976710656L, 351843720888320L, 422212465065984L,
                492581209243648L, 562949953421312L, 703687441776640L, 844424930131968L, 985162418487296L, 1125899906842624L,
                1407374883553280L, 1688849860263936L, 1970324836974592L, 2251799813685248L, 2814749767106560L, 3377699720527872L,
                3940649673949184L, 4503599627370496L, 5629499534213120L, 6755399441055744L, 7881299347898368L, 9007199254740992L,
                11258999068426240L, 13510798882111488L, 15762598695796736L, 18014398509481984L, 22517998136852480L, 27021597764222976L,
                31525197391593472L, 36028797018963968L, 45035996273704960L, 54043195528445952L, 63050394783186944L, 72057594037927936L,
                90071992547409920L, 108086391056891904L, 126100789566373888L, 144115188075855872L, 180143985094819840L, 216172782113783808L,
                252201579132747776L, 288230376151711744L, 360287970189639680L, 432345564227567616L, 504403158265495552L, 576460752303423488L,
                720575940379279360L, 864691128455135232L, 1008806316530991104L, 1152921504606846976L, 1441151880758558720L, 1729382256910270464L,
                2017612633061982208L, 2305843009213693952L, 2882303761517117440L, 3458764513820540928L, 4035225266123964416L, 4611686018427387904L,
                5764607523034234880L, 6917529027641081856L, 8070450532247928832L
        };
    }
}