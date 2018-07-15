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

package com.moilioncircle.redis.cli.tool.ext.rdt;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.AbstractRdbVisitor;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Guard;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Baoyi Chen
 */
public class BackupRdbVisitor extends AbstractRdbVisitor {
    
    public BackupRdbVisitor(Replicator replicator, Configure configure, List<Long> db, List<String> regexs, List<DataType> types, Supplier<OutputStream> supplier) {
        super(replicator, configure, db, regexs, types, supplier);
        this.replicator.addEventListener((rep, event) -> {
            if (event instanceof PreRdbSyncEvent) {
                listener.reset(supplier.get());
            }
            if (event instanceof PostRdbSyncEvent) {
                CRCOutputStream out = listener.getOutputStream();
                OutputStreams.writeQuietly(0xFF, out);
                OutputStreams.writeQuietly(out.getCRC64(), out);
                OutputStreams.closeQuietly(out);
            }
        });
    }
    
    @Override
    public int applyVersion(RedisInputStream in) throws IOException {
        listener.setGuard(Guard.DRAIN);
        try {
            return super.applyVersion(in);
        } finally {
            listener.setGuard(Guard.SAVE);
        }
    }
    
    @Override
    public Event applyAux(RedisInputStream in, int version) throws IOException {
        listener.setGuard(Guard.DRAIN);
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
}
