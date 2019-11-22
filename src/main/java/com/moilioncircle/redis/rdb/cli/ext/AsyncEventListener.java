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
import java.util.concurrent.ThreadFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.cmd.ClosedCommand;
import com.moilioncircle.redis.rdb.cli.ext.cmd.ClosingCommand;
import com.moilioncircle.redis.rdb.cli.util.XThreadFactory;
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

    private int count;
    private final int threads;
    private CyclicBarrier rdbBarrier;
    private CyclicBarrier closeBarrier;
    private final EventListener listener;
    private ExecutorService[] executors;

    public AsyncEventListener(EventListener listener, Replicator r, Configure c) {
        this.listener = listener;
        this.threads = c.getMigrateThreads();
        if (threads > 0) {
            if ((threads & (threads - 1)) != 0) {
                throw new IllegalArgumentException("migrate_thread_size " + threads + " must power of 2");
            }

            // 1
            this.rdbBarrier = new CyclicBarrier(threads);

            // 2
            this.closeBarrier = new CyclicBarrier(threads, () -> {
                this.listener.onEvent(r, new ClosedCommand());
            });

            // 3
            this.executors = new ScheduledExecutorService[threads];
            ThreadFactory factory = new XThreadFactory("sync-worker");
            for (int i = 0; i < this.executors.length; i++) {
                this.executors[i] = Executors.newSingleThreadScheduledExecutor(factory);
            }

            r.addCloseListener(rep -> {
                for (int i = 0; i < this.executors.length; i++) {
                    this.executors[i].submit(() -> {
                        this.listener.onEvent(r, new ClosingCommand());
                        await(closeBarrier);
                    });
                    terminateQuietly(this.executors[i], 0, MILLISECONDS);
                }
            });

        } else {
            r.addCloseListener(rep -> {
                this.listener.onEvent(r, new ClosingCommand());
                this.listener.onEvent(r, new ClosedCommand());
            });
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
                    reset(rdbBarrier);
                }

                // 2
                for (int i = 0; i < this.executors.length; i++) {
                    this.executors[i].submit(() -> this.listener.onEvent(replicator, event));
                }

                // 3
                if (event instanceof PostRdbSyncEvent) {
                    for (int i = 0; i < this.executors.length; i++) {
                        this.executors[i].submit(() -> await(rdbBarrier));
                    }
                }
            } else if (event instanceof DumpKeyValuePair) {
                int i = count++ & (executors.length - 1);
                this.executors[i].submit(() -> this.listener.onEvent(replicator, event));
            } else if (event instanceof Command) {
                // at this point all rdb event process done controlled by rdbBarrier.
                // so we can process aof event use thread 0 safely.
                this.executors[0].submit(() -> this.listener.onEvent(replicator, event));
            }
        } else {
            this.listener.onEvent(replicator, event);
        }
    }

    private void reset(CyclicBarrier barrier) {
        if (barrier != null) {
            barrier.reset();
        }
    }

    private void await(CyclicBarrier barrier) {
        try {
            if (barrier != null) {
                barrier.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
        }
    }
}