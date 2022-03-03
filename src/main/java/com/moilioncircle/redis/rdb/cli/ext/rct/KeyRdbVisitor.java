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

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class KeyRdbVisitor extends AbstractRdbVisitor {
    
    public KeyRdbVisitor(Replicator replicator, Configure configure, File out, Filter filter, Escaper escaper) {
        super(replicator, configure, out, filter, escaper);
    }
    
    @Override
    public Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyString(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyList(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplySet(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyZSet(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyZSet2(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyHash(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyHashZipMap(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyListZipList(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplySetIntSet(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyZSetZipList(in, version, key, contains, type, context);
    }
    
    @Override
    protected Event doApplyZSetListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyZSetListPack(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyHashZipList(in, version, key, contains, type, context);
    }
    
    @Override
    protected Event doApplyHashListPack(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyHashListPack(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyListQuickList(in, version, key, contains, type, context);
    }
    
    @Override
    protected Event doApplyListQuickList2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyListQuickList2(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyModule(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyModule2(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyStreamListPacks(in, version, key, contains, type, context);
    }
    
    @Override
    public Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        quote(key, out);
        OutputStreams.write('\n', out);
        return super.doApplyStreamListPacks2(in, version, key, contains, type, context);
    }
}