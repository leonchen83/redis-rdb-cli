/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Baoyi Chen
 */
public class XThreadFactory implements ThreadFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(XThreadFactory.class);

    private final String name;
    private final boolean daemon;
    private final AtomicLong sequences = new AtomicLong();
    private final Thread.UncaughtExceptionHandler handler;

    public XThreadFactory(String name) {
        this(name, false, null);
    }

    public XThreadFactory(String name, boolean daemon) {
        this(name, daemon, null);
    }

    public XThreadFactory(String name, boolean daemon, Thread.UncaughtExceptionHandler handler) {
        this.name = name;
        this.daemon = daemon;
        this.handler = handler;
    }

    /**
     *
     */
    @Override
    public Thread newThread(final Runnable task) {
        //
        final Thread r = new Thread(task);
        r.setDaemon(daemon);

        //
        String prefix = this.name;
        r.setName(prefix + "-" + sequences.incrementAndGet());

        if (handler != null) {
            r.setUncaughtExceptionHandler(handler);
        } else {
            r.setUncaughtExceptionHandler(new Handler());
        }
        return r;
    }

    private static final class Handler implements Thread.UncaughtExceptionHandler {
        public void uncaughtException(final Thread t, final Throwable tx) {
            logger.error("unhandled exception: " + t.getId() + "@" + t.getName(), tx);
        }
    }
}
