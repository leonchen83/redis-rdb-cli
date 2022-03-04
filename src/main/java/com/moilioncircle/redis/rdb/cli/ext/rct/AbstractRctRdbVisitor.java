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

import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.visitor.BaseRdbVisitor;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractRctRdbVisitor extends BaseRdbVisitor {
	
	protected Escaper escaper;
	protected OutputStream out;
	
	public AbstractRctRdbVisitor(Replicator replicator, Configure configure, Filter filter, File output, Escaper escaper) {
		super(replicator, configure, filter);
		this.escaper = escaper;
		replicator.addEventListener((rep, event) -> {
			if (event instanceof PreRdbSyncEvent) {
				OutputStreams.closeQuietly(this.out);
				this.out = OutputStreams.newBufferedOutputStream(output, configure.getOutputBufferSize());
			}
		});
		replicator.addCloseListener(rep -> OutputStreams.closeQuietly(out));
	}
	
	protected void delimiter(OutputStream out) {
		OutputStreams.write(configure.getDelimiter(), out);
	}
	
	protected void quote(byte[] bytes, OutputStream out) {
		quote(bytes, out, true);
	}
	
	protected void quote(byte[] bytes, OutputStream out, boolean escape) {
		OutputStreams.write(configure.getQuote(), out);
		if (escape) {
			this.escaper.encode(bytes, out);
		} else {
			OutputStreams.write(bytes, out);
		}
		OutputStreams.write(configure.getQuote(), out);
	}
	
	protected Event doApplyStreamListPacks2(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context, RawByteListener listener) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long listPacks = skipParser.rdbLoadLen().len;
		while (listPacks-- > 0) {
			skipParser.rdbLoadPlainStringObject();
			skipParser.rdbLoadPlainStringObject();
		}
		skipParser.rdbLoadLen(); // length
		skipParser.rdbLoadLen(); // lastId
		skipParser.rdbLoadLen(); // lastId
		if (version < 10) replicator.removeRawByteListener(listener);
		skipParser.rdbLoadLen(); // firstId
		skipParser.rdbLoadLen(); // firstId
		skipParser.rdbLoadLen(); // maxDeletedEntryId
		skipParser.rdbLoadLen(); // maxDeletedEntryId
		skipParser.rdbLoadLen(); // entriesAdded
		if (version < 10) replicator.addRawByteListener(listener);
		long groupCount = skipParser.rdbLoadLen().len;
		while (groupCount-- > 0) {
			skipParser.rdbLoadPlainStringObject();
			skipParser.rdbLoadLen();
			skipParser.rdbLoadLen();
			if (version < 10) replicator.removeRawByteListener(listener);
			skipParser.rdbLoadLen(); // entriesRead
			if (version < 10) replicator.addRawByteListener(listener);
			long groupPel = skipParser.rdbLoadLen().len;
			while (groupPel-- > 0) {
				in.skip(16);
				skipParser.rdbLoadMillisecondTime();
				skipParser.rdbLoadLen();
			}
			long consumerCount = skipParser.rdbLoadLen().len;
			while (consumerCount-- > 0) {
				skipParser.rdbLoadPlainStringObject();
				skipParser.rdbLoadMillisecondTime();
				long consumerPel = skipParser.rdbLoadLen().len;
				while (consumerPel-- > 0) {
					in.skip(16);
				}
			}
		}
		if (version < 10) context.setValueRdbType(RDB_TYPE_STREAM_LISTPACKS);
		return context.valueOf(new DummyKeyValuePair());
	}
}
