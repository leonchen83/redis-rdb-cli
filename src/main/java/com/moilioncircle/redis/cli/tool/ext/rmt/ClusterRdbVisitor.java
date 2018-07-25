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
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.moilioncircle.redis.cli.tool.net.Endpoint.closeQuietly;
import static com.moilioncircle.redis.cli.tool.util.CRC16.crc16;
import static java.util.Collections.singletonList;

/**
 * @author Baoyi Chen
 */
public class ClusterRdbVisitor extends AbstractRdbVisitor implements EventListener {

    private final File conf;
    private final boolean replace;
    private Map<Short, Endpoint> endpoints = new HashMap<>();

    public ClusterRdbVisitor(Replicator replicator, Configure configure, File conf, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) {
        super(replicator, configure, singletonList(0L), regexs, types);
        this.conf = conf;
        this.replace = replace;
        this.replicator.addEventListener(this);
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            Set<Endpoint> set = new HashSet<>(this.endpoints.values());
            set.forEach(e -> closeQuietly(e));
            int pipe = configure.getMigrateBatchSize();
            this.endpoints = Endpoint.valueOf(conf, pipe);
        } else if (event instanceof DumpKeyValuePair) {
            retry(event, configure.getMigrateRetryTime());
        } else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            Set<Endpoint> set = new HashSet<>(this.endpoints.values());
            set.forEach(e -> {
                e.flush();
                closeQuietly(e);
            });
        }
    }

    protected short slot(byte[] key) {
        if (key == null) return 0;
        int st = -1, ed = -1;
        for (int i = 0, len = key.length; i < len; i++) {
            if (key[i] == '{' && st == -1) st = i;
            if (key[i] == '}' && st >= 0) {
                ed = i;
                break;
            }
        }
        if (st >= 0 && ed >= 0 && ed > st + 1)
            return (short) (crc16(key, st + 1, ed) & 16383);
        return (short) (crc16(key) & 16383);
    }

    public void retry(Event event, int times) {
        DumpKeyValuePair dkv = (DumpKeyValuePair) event;
        short slot = slot(dkv.getKey());
        Endpoint endpoint = this.endpoints.get(slot);
        try {
            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) return;
                expire = String.valueOf(ms).getBytes();
            }

            if (!replace) {
                endpoint.batch(true, RESTORE_ASKING, dkv.getKey(), expire, dkv.getValue());
            } else {
                endpoint.batch(true, RESTORE_ASKING, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        } catch (Throwable e) {
            times--;
            if (times >= 0) {
                this.endpoints.put(slot, Endpoint.valueOf(endpoint));
                retry(event, times);
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
