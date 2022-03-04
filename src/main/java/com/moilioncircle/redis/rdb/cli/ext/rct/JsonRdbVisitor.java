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

package com.moilioncircle.redis.rdb.cli.ext.rct;

import java.io.File;
import java.io.IOException;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.io.RedisInputStream;

/**
 * @author Baoyi Chen
 */
public class JsonRdbVisitor extends AbstractJsonRdbVisitor {
    
    public JsonRdbVisitor(Replicator replicator, Configure configure, Filter filter, File output, Escaper escaper) {
        super(replicator, configure, filter, output, escaper);
    }

    @Override
    public String applyMagic(RedisInputStream in) throws IOException {
        Outputs.write('[', out);
        return super.applyMagic(in);
    }

    @Override
    public long applyEof(RedisInputStream in, int version) throws IOException {
        Outputs.write(']', out);
        return super.applyEof(in, version);
    }

    /**
     *
     */
    protected void separator() {
        Outputs.write(',', out);
        Outputs.write('\n', out);
    }
    
}
