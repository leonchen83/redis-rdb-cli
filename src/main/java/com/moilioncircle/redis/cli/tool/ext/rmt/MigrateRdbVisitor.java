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

package com.moilioncircle.redis.cli.tool.ext.rmt;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.DumpRawByteListener;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.net.Endpoint;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.moilioncircle.redis.cli.tool.net.Endpoint.closeQuietly;
import static com.moilioncircle.redis.replicator.Configuration.valueOf;

/**
 * @author Baoyi Chen
 */
public class MigrateRdbVisitor extends AbstractRdbVisitor implements EventListener {
    
    private Endpoint endpoint;
    private final RedisURI uri;
    private final boolean replace;
    private final Configuration conf;
    private final AtomicInteger dbnum = new AtomicInteger(0);
    
    public MigrateRdbVisitor(Replicator replicator, Configure configure, String uri, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) throws Exception {
        super(replicator, configure, db, regexs, types);
        this.replace = replace;
        this.uri = new RedisURI(uri);
        this.replicator.addEventListener(this);
        this.conf = configure.merge(valueOf(this.uri));
        this.replicator.addCloseListener(e -> closeQuietly(this.endpoint));
    }
    
    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            closeQuietly(this.endpoint);
            this.endpoint = new Endpoint(uri.getHost(), uri.getPort(), 0, configure.getMigrateBatchSize(), conf);
            dbnum.set(0);
        } else if (event instanceof PostRdbSyncEvent) {
            this.endpoint.flush();
        } else if (event instanceof DumpKeyValuePair) {
            DumpKeyValuePair dkv = (DumpKeyValuePair) event;
            DB db = dkv.getDb();
            int index;
            if (db != null && (index = (int) db.getDbNumber()) != dbnum.get()) {
                endpoint.batch(SELECT, String.valueOf(index).getBytes());
                dbnum.set(index);
            }
    
            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) return;
                expire = String.valueOf(ms).getBytes();
            }
            if (!replace) {
                endpoint.batch(RESTORE, dkv.getKey(), expire, dkv.getValue());
            } else {
                endpoint.batch(RESTORE, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        }
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyString(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplySet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSet2(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHash(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHashZipMap(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyListZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplySetIntSet(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyZSetZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyHashZipList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyListQuickList(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyModule2(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, int version, byte[] key, boolean contains, int type, ContextKeyValuePair context) throws IOException {
        int ver = configure.getDumpRdbVersion() == -1 ? version : configure.getDumpRdbVersion();
        try (ByteArrayOutputStream o = new ByteArrayOutputStream(configure.getBufferSize())) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) type, ver, o, Escape.RAW, configure)) {
                replicator.addRawByteListener(listener);
                super.doApplyStreamListPacks(in, version, key, contains, type, context);
                replicator.removeRawByteListener(listener);
            }
            DumpKeyValuePair dump = new DumpKeyValuePair();
            dump.setValueRdbType(type);
            dump.setKey(key);
            dump.setValue(o.toByteArray());
            return context.valueOf(dump);
        }
    }
}
