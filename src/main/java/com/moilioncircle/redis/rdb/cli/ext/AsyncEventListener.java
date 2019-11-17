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

package com.moilioncircle.redis.rdb.cli.ext;

import static com.moilioncircle.redis.replicator.util.Concurrents.terminateQuietly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.cmd.CloseCommand;
import com.moilioncircle.redis.rdb.cli.ext.cmd.GuardCommand;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;

/**
 * @author Baoyi Chen
 */
public class AsyncEventListener implements EventListener {

    private static final Logger logger = LoggerFactory.getLogger(MonitorManager.class);

    private int count;
    private final int threads; 
    private CyclicBarrier barrier;
    private MonitorManager manager;
    private final EventListener listener;
    private ExecutorService[] executors;

    public AsyncEventListener(EventListener listener, Replicator r, Configure c, MonitorManager manager) {
        this.manager = manager;
        this.listener = listener;
        this.threads = c.getMigrateThreads();
        if (threads > 0) {
            if ((threads & (threads - 1)) != 0) {
                throw new IllegalArgumentException("migrate_thread_size " + threads + " must power of 2");
            }
            this.executors = new ScheduledExecutorService[threads];
            for (int i = 0; i < this.executors.length; i++) {
                this.executors[i] = Executors.newSingleThreadScheduledExecutor();
            }
            r.addCloseListener(rep -> {
                for (int i = 0; i < this.executors.length; i++) {
                    this.executors[i].submit(() -> this.listener.onEvent(r, new CloseCommand()));
                    terminateQuietly(this.executors[i], 0, MILLISECONDS);
                }
            });
            this.barrier = new CyclicBarrier(threads);
        }
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (threads > 0) {
            if (event instanceof PreRdbSyncEvent ||
                    event instanceof PostRdbSyncEvent ||
                    event instanceof PreCommandSyncEvent ||
                    event instanceof PostCommandSyncEvent) {
                // 1
                if (event instanceof PreRdbSyncEvent) {
                    reset();
                    manager.reset();
                }
                
                // 2
                for (int i = 0; i < this.executors.length; i++) {
                    this.executors[i].submit(() -> this.listener.onEvent(replicator, event));
                }
                
                // 3
                if (event instanceof PostRdbSyncEvent) {
                    for (int i = 0; i < this.executors.length; i++) {
                        this.executors[i].submit(() -> await());
                        this.executors[i].submit(() -> this.listener.onEvent(replicator, new GuardCommand()));
                    }
                }
            } else if (event instanceof DumpKeyValuePair) {
                int i = count++ & (executors.length - 1);
                this.executors[i].submit(() -> this.listener.onEvent(replicator, event));
            } else if (event instanceof Command) {
                // at this point all rdb event process done controlled by barrier.
                // so we can process aof event use thread 0 safely.
                this.executors[0].submit(() -> this.listener.onEvent(replicator, event));
            }
        } else {
            if (event instanceof PreRdbSyncEvent) {
                manager.reset();
            }
            this.listener.onEvent(replicator, event);
        }
    }
    
    private void reset() {
        if (barrier != null) {
            barrier.reset();
            logger.debug("barrier reset");
        }
    }

    private void await() {
        try {
            if (barrier != null) {
                barrier.await();
                logger.debug("barrier await");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
        }
    }
}