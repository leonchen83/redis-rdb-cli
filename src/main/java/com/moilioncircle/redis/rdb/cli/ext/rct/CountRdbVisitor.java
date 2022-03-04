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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
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

    public CountRdbVisitor(Replicator replicator, Configure configure, Filter filter, File output, Escaper escaper) {
        super(replicator, configure, filter, output, escaper);
        replicator.addEventListener(this);
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            List<String> keys = new ArrayList<>(counter.keySet());
            for (String key : keys) {
                OutputStreams.write(key.getBytes(), out);
                delimiter(out);
            }
            OutputStreams.write("summary".getBytes(), out);
            OutputStreams.write('\n', out);
            long total = 0;
            for (String key : keys) {
                long t = counter.get(key);
                total += t;
                OutputStreams.write(String.valueOf(t).getBytes(), out);
                delimiter(out);
            }
            OutputStreams.write(String.valueOf(total).getBytes(), out);
            OutputStreams.write('\n', out);
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
}
