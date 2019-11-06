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

package com.moilioncircle.redis.rdb.cli.ext.rmt;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.AsyncEventListener;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.metric.MetricJobs;
import com.moilioncircle.redis.rdb.cli.net.Endpoint;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.moilioncircle.redis.rdb.cli.metric.MetricReporterFactory.create;

/**
 * @author Baoyi Chen
 */
public class SingleRdbVisitor extends AbstractMigrateRdbVisitor implements EventListener {
    
    private final RedisURI uri;
    private final boolean legacy;
    private volatile byte[] evalSha;
    private final Configuration conf;
    private ThreadLocal<Endpoint> endpoint = new ThreadLocal<>();
    
    public SingleRdbVisitor(Replicator replicator,
                            Configure configure,
                            RedisURI uri,
                            List<Long> db,
                            List<String> regexs,
                            List<DataType> types,
                            boolean replace,
                            boolean legacy) throws Exception {
        super(replicator, configure, db, regexs, types, replace);
        this.uri = uri;
        this.legacy = legacy;
        this.conf = configure.merge(this.uri);
        this.replicator.addEventListener(new AsyncEventListener(this, replicator, configure));
    }
    
    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof PreRdbSyncEvent) {
            Endpoint.closeQuietly(this.endpoint.get());
            int pipe = configure.getMigrateBatchSize();
            this.endpoint.set(new Endpoint(uri.getHost(), uri.getPort(), 0, pipe, registry, conf));
    
            if (this.reporter != null) this.reporter.close();
            this.reporter = create(configure, registry, MetricJobs.endpoint(configure));
            this.reporter.start(5, TimeUnit.SECONDS);
        } else if (event instanceof DumpKeyValuePair) {
            retry(event, configure.getMigrateRetries());
        } else if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent) {
            this.endpoint.get().flush();
            Endpoint.closeQuietly(this.endpoint.get());
    
            if (this.reporter != null) {
                this.reporter.report();
                this.reporter.close();
            }
        } else if (event instanceof AsyncEventListener.Syncer) {
            ((AsyncEventListener.Syncer)event).await();
        } else if (event instanceof Command) {
            // TODO
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
            } else if (legacy) {
                // https://github.com/leonchen83/redis-rdb-cli/issues/6
                eval(dkv.getKey(), dkv.getValue(), expire);
            } else {
                endpoint.get().batch(flush, RESTORE, dkv.getKey(), expire, dkv.getValue(), REPLACE);
            }
        } catch (Throwable e) {
            times--;
            if (times >= 0 && flush) {
                endpoint.set(Endpoint.valueOf(endpoint.get()));
                retry(event, times);
            }
        }
    }
    
    /*
     * LUA
     * step 1 : SCRIPT LOAD script
     * step 2 : EVALSHA sha1
     */
    private static final byte[] LUA_SCRIPT =
            ("redis.call('del',KEYS[1]);return redis.call('restore',KEYS[1],ARGV[1],ARGV[2]);").getBytes();
    
    protected void eval(byte[] key, byte[] value, byte[] expire) {
        if (evalSha == null) {
            endpoint.get().flush(); // flush prev commands so that to read script.
            Endpoint.RedisObject r = endpoint.get().send(SCRIPT, LOAD, LUA_SCRIPT);
            byte[] evalSha = r.getBytes();
            if (evalSha == null) throw new RuntimeException(); // retry in the caller method.
            this.evalSha = evalSha;
            eval(key, value, expire);
        } else {
            endpoint.get().batch(flush, EVALSHA, evalSha, ONE, key, expire, value);
        }
    }
}
