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

package com.moilioncircle.redis.rdb.cli.ext.rdt;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.Guard;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.Slot;

/**
 * @author Baoyi Chen
 */
public class MergeRdbVisitor extends AbstractRdtRdbVisitor {
    
    public MergeRdbVisitor(Replicator replicator, Configure configure, Args.RdtArgs arg, Supplier<OutputStream> supplier) {
        super(replicator, configure, arg.filter, supplier);
    }
    
    @Override
    public int applyVersion(RedisInputStream in) throws IOException {
        listener.setGuard(Guard.PASS);
        try {
            return super.applyVersion(in);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Event applyAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.PASS);
        try {
            return super.applyAux(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Event applyModuleAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyModuleAux(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Event applyFunction(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyFunction(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Event applyFunction2(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyFunction2(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applySelectDB(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyResizeDB(in, version, context);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public long applyEof(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.PASS);
        try {
            return super.applyEof(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Slot applySlotInfo(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.PASS);
        try {
            return super.applySlotInfo(in, version);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
}
