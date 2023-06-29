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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.glossary.FileType;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class CountRdbVisitor extends AbstractRctRdbVisitor implements EventListener {

    private Map<String, Long> counter = new HashMap<>();
    
    //noinspection ThisEscapedInObjectConstruction
    public CountRdbVisitor(Replicator replicator, Configure configure, Args.RctArgs args, Escaper escaper) {
        super(replicator, configure, args, escaper);
        replicator.addEventListener(this);
    }
    
    private void exportCsv(Map<String, Long> counter) {
        List<String> keys = new ArrayList<>(counter.keySet());
        for (String key : keys) {
            Outputs.write(key.getBytes(), out);
            delimiter(out);
        }
        Outputs.write("summary".getBytes(), out);
        Outputs.write('\n', out);
        long total = 0;
        for (String key : keys) {
            long t = counter.get(key);
            total += t;
            Outputs.write(String.valueOf(t).getBytes(), out);
            delimiter(out);
        }
        Outputs.write(String.valueOf(total).getBytes(), out);
        Outputs.write('\n', out);
    }
    
    private void exportJsonl(Map<String, Long> counter) {
        Outputs.write('{', out);
        long total = 0L;
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            emitField(entry.getKey(), count);
            Outputs.write(',', out);
            total += count;
        }
        emitField("summary", total);
        Outputs.write('}', out);
        Outputs.write('\n', out);
    }
    
    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            if (configure.getExportFileFormat() == FileType.CSV) {
                exportCsv(counter);
            } else if (configure.getExportFileFormat() == FileType.JSONL) {
                exportJsonl(counter);
            }
        }
    }

    @Override
    public Event doApplyString(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyString(in, version, key, type, context);
    }

    @Override
    public Event doApplyList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyList(in, version, key, type, context);
    }

    @Override
    public Event doApplySet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplySet(in, version, key, type, context);
    }
    
    @Override
    public Event doApplySetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplySetListPack(in, version, key, type, context);
    }

    @Override
    public Event doApplyZSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyZSet(in, version, key, type, context);
    }

    @Override
    public Event doApplyZSet2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyZSet2(in, version, key, type, context);
    }

    @Override
    public Event doApplyHash(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyHash(in, version, key, type, context);
    }

    @Override
    public Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyHashZipMap(in, version, key, type, context);
    }

    @Override
    public Event doApplyListZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyListZipList(in, version, key, type, context);
    }

    @Override
    public Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplySetIntSet(in, version, key, type, context);
    }

    @Override
    public Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyZSetZipList(in, version, key, type, context);
    }
    
    @Override
    public Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyZSetListPack(in, version, key, type, context);
    }

    @Override
    public Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyHashZipList(in, version, key, type, context);
    }
    
    @Override
    public Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyHashListPack(in, version, key, type, context);
    }

    @Override
    public Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyListQuickList(in, version, key, type, context);
    }
    
    @Override
    public Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyListQuickList2(in, version, key, type, context);
    }

    @Override
    public Event doApplyModule(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyModule(in, version, key, type, context);
    }

    @Override
    public Event doApplyModule2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyModule2(in, version, key, type, context);
    }

    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyStreamListPacks(in, version, key, type, context);
    }
    
    @Override
    public Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyStreamListPacks2(in, version, key, type, context);
    }
    
    @Override
    public Event doApplyStreamListPacks3(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context) throws IOException {
        counter.compute(DataType.parse(type).getValue(), (k, v) -> v == null ? 1 : v + 1);
        return super.doApplyStreamListPacks3(in, version, key, type, context);
    }
}
