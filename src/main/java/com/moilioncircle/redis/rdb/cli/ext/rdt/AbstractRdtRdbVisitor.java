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

package com.moilioncircle.redis.rdb.cli.ext.rdt;

import java.io.OutputStream;
import java.util.function.Supplier;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.GuardRawByteListener;
import com.moilioncircle.redis.rdb.cli.ext.visitor.BaseRdbVisitor;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.replicator.Replicator;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractRdtRdbVisitor extends BaseRdbVisitor {
	
	public AbstractRdtRdbVisitor(Replicator replicator, Configure configure, Filter filter, Supplier<OutputStream> supplier) {
		super(replicator, configure, filter);
		listener = new GuardRawByteListener(configure.getOutputBufferSize(), supplier.get());
		replicator.addRawByteListener(listener);
	}
}
