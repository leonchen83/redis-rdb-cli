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

import java.io.IOException;
import java.io.OutputStream;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.datatype.DummyKeyValuePair;
import com.moilioncircle.redis.rdb.cli.ext.visitor.BaseRdbVisitor;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
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
	
	public AbstractRctRdbVisitor(Replicator replicator, Configure configure, Args.RctArgs args, Escaper escaper) {
		super(replicator, configure, args.filter);
		this.escaper = escaper;
		replicator.addEventListener((rep, event) -> {
			if (event instanceof PreRdbSyncEvent) {
				Outputs.closeQuietly(this.out);
				this.out = Outputs.newBufferedOutput(args.output, configure.getOutputBufferSize());
			}
		});
		replicator.addCloseListener(rep -> Outputs.closeQuietly(out));
	}
	
	protected void delimiter(OutputStream out) {
		Outputs.write(configure.getDelimiter(), out);
	}
	
	protected void quote(byte[] bytes, OutputStream out) {
		quote(bytes, out, true);
	}
	
	protected void quote(byte[] bytes, OutputStream out, boolean escape) {
		Outputs.write(configure.getQuote(), out);
		if (escape) {
			this.escaper.encode(bytes, out);
		} else {
			Outputs.write(bytes, out);
		}
		Outputs.write(configure.getQuote(), out);
	}
	
	protected void emitField(String field, long value) {
		emitString(field.getBytes());
		Outputs.write(':', out);
		escaper.encode(String.valueOf(value).getBytes(), out);
	}
	
	protected void emitField(String field, byte[] value) {
		emitField(field.getBytes(), value);
	}
	
	protected void emitField(String field, String value) {
		emitField(field.getBytes(), value.getBytes());
	}
	
	protected void emitField(byte[] field, byte[] value) {
		emitString(field);
		Outputs.write(':', out);
		emitString(value);
	}
	
	protected void emitNull(byte[] field) {
		emitString(field);
		Outputs.write(':', out);
		escaper.encode("null".getBytes(), out);
	}
	
	protected void emitZSet(byte[] field, double value) {
		emitString(field);
		Outputs.write(':', out);
		escaper.encode(value, out);
	}
	
	protected void emitString(byte[] str) {
		Outputs.write('"', out);
		escaper.encode(str, out);
		Outputs.write('"', out);
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
	
	protected Event doApplyStreamListPacks3(RedisInputStream in, int version, byte[] key, int type, ContextKeyValuePair context, RawByteListener listener) throws IOException {
		SkipRdbParser skipParser = new SkipRdbParser(in);
		long listPacks = skipParser.rdbLoadLen().len;
		while (listPacks-- > 0) {
			skipParser.rdbLoadPlainStringObject();
			skipParser.rdbLoadPlainStringObject();
		}
		skipParser.rdbLoadLen(); // length
		skipParser.rdbLoadLen(); // lastId
		skipParser.rdbLoadLen(); // lastId
		if (version < 11) replicator.removeRawByteListener(listener);
		skipParser.rdbLoadLen(); // firstId
		skipParser.rdbLoadLen(); // firstId
		skipParser.rdbLoadLen(); // maxDeletedEntryId
		skipParser.rdbLoadLen(); // maxDeletedEntryId
		skipParser.rdbLoadLen(); // entriesAdded
		if (version < 11) replicator.addRawByteListener(listener);
		long groupCount = skipParser.rdbLoadLen().len;
		while (groupCount-- > 0) {
			skipParser.rdbLoadPlainStringObject();
			skipParser.rdbLoadLen();
			skipParser.rdbLoadLen();
			if (version < 11) replicator.removeRawByteListener(listener);
			skipParser.rdbLoadLen(); // entriesRead
			if (version < 11) replicator.addRawByteListener(listener);
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
				if (version < 11) replicator.removeRawByteListener(listener);
				skipParser.rdbLoadMillisecondTime(); // activeTime
				if (version < 11) replicator.addRawByteListener(listener);
				long consumerPel = skipParser.rdbLoadLen().len;
				while (consumerPel-- > 0) {
					in.skip(16);
				}
			}
		}
		if (version < 11) context.setValueRdbType(RDB_TYPE_STREAM_LISTPACKS);
		return context.valueOf(new DummyKeyValuePair());
	}
}
