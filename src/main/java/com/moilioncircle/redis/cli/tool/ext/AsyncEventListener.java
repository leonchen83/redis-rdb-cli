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

package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.moilioncircle.redis.replicator.util.Concurrents.terminateQuietly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Baoyi Chen
 */
public class AsyncEventListener implements EventListener {

    private final EventListener listener;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public AsyncEventListener(EventListener listener, Replicator r, Configure c) {
        this.listener = listener;
        r.addCloseListener(rep -> terminateQuietly(executor, c.getTimeout(), MILLISECONDS));
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        this.executor.submit(() -> this.listener.onEvent(replicator, event));
    }
}