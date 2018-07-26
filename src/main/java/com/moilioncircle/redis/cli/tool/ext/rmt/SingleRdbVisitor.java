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
import com.moilioncircle.redis.cli.tool.ext.AsyncEventListener;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.net.Endpoint;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

import java.util.List;

import static com.moilioncircle.redis.cli.tool.net.Endpoint.closeQuietly;

/**
 * @author Baoyi Chen
 */
public class SingleRdbVisitor extends AbstractMigrateRdbVisitor implements EventListener {

    private final RedisURI uri;
    private final Configuration conf;
    private ThreadLocal<Endpoint> endpoint = new ThreadLocal<>();

    public SingleRdbVisitor(Replicator replicator, Configure configure, String uri, List<Long> db, List<String> regexs, List<DataType> types, boolean replace) throws Exception {
        super(replicator, configure, db, regexs, types, replace);
        this.uri = new RedisURI(uri);
        this.conf = configure.merge(this.uri);
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure));
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            closeQuietly(this.endpoint.get());
            int pipe = configure.getMigrateBatchSize();
            this.endpoint.set(new Endpoint(uri.getHost(), uri.getPort(), 0, pipe, conf));
        } else if (event instanceof DumpKeyValuePair) {
            retry(event, configure.getMigrateRetries());
        } else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            this.endpoint.get().flush();
            closeQuietly(this.endpoint.get());
        }
    }

    public void retry(Event event, int times) {
        try {
            DumpKeyValuePair dkv = (DumpKeyValuePair) event;
            DB db = dkv.getDb();

            int index;
            if (db != null && (index = (int) db.getDbNumber()) != endpoint.get().getDB()) {
                endpoint.get().select(true, index);
            }

            byte[] expire = ZERO;
            if (dkv.getExpiredMs() != null) {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) return;
                expire = String.valueOf(ms).getBytes();
            }
            if (!replace) {
                endpoint.get().batch(flush, RESTORE, dkv.getKey(), expire, dkv.getValue());
            } else {
                endpoint.get().batch(flush, RESTORE, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        } catch (Throwable e) {
            times--;
            if (times >= 0) {
                endpoint.set(Endpoint.valueOf(endpoint.get()));
                retry(event, times);
            }
        }
    }
}
