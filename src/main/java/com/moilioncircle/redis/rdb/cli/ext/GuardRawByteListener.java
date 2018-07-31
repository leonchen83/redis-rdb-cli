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

import com.moilioncircle.redis.rdb.cli.glossary.Guard;
import com.moilioncircle.redis.rdb.cli.io.GuardOutputStream;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class GuardRawByteListener implements RawByteListener {
    private GuardOutputStream out;
    private OutputStream internal;
    
    public GuardRawByteListener(int cap, OutputStream internal) {
        this.internal = internal;
        this.out = new GuardOutputStream(cap, internal);
    }
    
    public <T extends OutputStream> T getOutputStream() {
        return (T) this.internal;
    }
    
    public void reset(OutputStream out) {
        this.internal = out;
        this.out.reset(out);
    }
    
    public void setGuard(Guard guard) {
        this.out.setGuard(guard);
    }
    
    @Override
    public void handle(byte... raw) {
        OutputStreams.write(raw, out);
    }
}