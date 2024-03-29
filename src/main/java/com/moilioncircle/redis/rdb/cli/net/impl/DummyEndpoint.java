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

package com.moilioncircle.redis.rdb.cli.net.impl;

import java.util.ArrayList;

import com.moilioncircle.redis.rdb.cli.net.AbstractEndpoint;
import com.moilioncircle.redis.replicator.Configuration;

/**
 * @author Baoyi Chen
 */
public class DummyEndpoint extends AbstractEndpoint {
    
    public DummyEndpoint(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return "<" + host + ":" + port + ">";
    }
    
    public static XEndpoint valueOf(DummyEndpoint dummy, Configuration conf, int pipe) {
        XEndpoint v = new XEndpoint(dummy.host, dummy.port, 0, pipe, true, conf);
        v.setSlots(new ArrayList<>(dummy.getSlots()));
        return v;
    }
    
    public static XEndpoint valueOfQuietly(DummyEndpoint dummy, Configuration conf, int pipe) {
        try {
            return valueOf(dummy, conf, pipe);
        } catch (Throwable e) {
            return null;
        }
    }
}
