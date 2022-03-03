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

package com.moilioncircle.redis.rdb.cli.net.protocol;

import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.DESCRIPTION_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.FUNCTION_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.LOAD_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.REPLACE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.RedisConstants.RESTORE_BUF;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.STAR;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.replicator.rdb.datatype.Function;

/**
 * @author Baoyi Chen
 */
public class Protocols {
	
	public static void emit(OutputStream out, ByteBuffer command, ByteBuffer... ary) {
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(ary.length + 1).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(command.remaining()).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(command.array(), command.position(), command.limit(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		for (final ByteBuffer arg : ary) {
			OutputStreams.write(DOLLAR, out);
			OutputStreams.write(String.valueOf(arg.remaining()).getBytes(), out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
			OutputStreams.write(arg.array(), arg.position(), arg.limit(), out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
		}
	}
	
	public static void emit(OutputStream out, ByteBuffers command, ByteBuffers... ary) {
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(ary.length + 1).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(command.getSize()).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		Iterator<ByteBuffer> cit = command.getBuffers();
		while (cit.hasNext()) {
			ByteBuffer tmp = cit.next();
			OutputStreams.write(tmp.array(), tmp.position(), tmp.limit(), out);
		}
		
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		for (final ByteBuffers arg : ary) {
			OutputStreams.write(DOLLAR, out);
			OutputStreams.write(String.valueOf(arg.getSize()).getBytes(), out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
			Iterator<ByteBuffer> ait = arg.getBuffers();
			while (ait.hasNext()) {
				ByteBuffer tmp = ait.next();
				OutputStreams.write(tmp.array(), tmp.position(), tmp.limit(), out);
			}
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
		}
	}
	
	public static void emit(OutputStream out, byte[] command, byte[]... ary) {
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(ary.length + 1).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(command.length).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(command, out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		for (final byte[] arg : ary) {
			OutputStreams.write(DOLLAR, out);
			OutputStreams.write(String.valueOf(arg.length).getBytes(), out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
			OutputStreams.write(arg, out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
		}
	}
	
	public static void emit(OutputStream out, byte[] command, byte[] key, List<byte[]> ary) {
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(ary.size() + 2).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(command.length).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(command, out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(key.length).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(key, out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		for (final byte[] arg : ary) {
			OutputStreams.write(DOLLAR, out);
			OutputStreams.write(String.valueOf(arg.length).getBytes(), out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
			OutputStreams.write(arg, out);
			OutputStreams.write('\r', out);
			OutputStreams.write('\n', out);
		}
	}
	
	public static void restore(OutputStream out, ByteBuffer key, ByteBuffer ex, ByteBuffers value, boolean replace) {
		OutputStreams.write(STAR, out);
		if (replace) {
			OutputStreams.write("5".getBytes(), out);
		} else {
			OutputStreams.write("4".getBytes(), out);
		}
		
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		
		emitArg(out, RESTORE_BUF);
		emitArg(out, key);
		emitArg(out, ex);
		emitArg(out, value);
		if (replace) {
			emitArg(out, REPLACE_BUF);
		}
	}
	
	public static void functionRestore(OutputStream out, ByteBuffers value, boolean replace) {
		OutputStreams.write(STAR, out);
		if (replace) {
			OutputStreams.write("4".getBytes(), out);
		} else {
			OutputStreams.write("3".getBytes(), out);
		}
		
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		emitArg(out, FUNCTION_BUF);
		emitArg(out, RESTORE_BUF);
		emitArg(out, value);
		if (replace) {
			emitArg(out, REPLACE_BUF);
		}
	}
	
	public static void functionLoad(OutputStream out, Function function, boolean replace) {
		int count = 5;
		if (function.getDescription() != null) {
			count += 2;
		}
		if (replace) {
			count += 1;
		}
		OutputStreams.write(STAR, out);
		OutputStreams.write(String.valueOf(count).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		emitArg(out, FUNCTION_BUF);
		emitArg(out, LOAD_BUF);
		emitArg(out, ByteBuffer.wrap(function.getEngineName()));
		emitArg(out, ByteBuffer.wrap(function.getName()));
		if (function.getDescription() != null) {
			emitArg(out, DESCRIPTION_BUF);
			emitArg(out, ByteBuffer.wrap(function.getDescription()));
		}
		if (replace) {
			emitArg(out, REPLACE_BUF);
		}
		emitArg(out, ByteBuffer.wrap(function.getCode()));
	}
	
	private static void emitArg(OutputStream out, ByteBuffer arg) {
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(arg.remaining()).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		OutputStreams.write(arg.array(), arg.position(), arg.limit(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
	}
	
	private static void emitArg(OutputStream out, ByteBuffers value) {
		OutputStreams.write(DOLLAR, out);
		OutputStreams.write(String.valueOf(value.getSize()).getBytes(), out);
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
		while (value.getBuffers().hasNext()) {
			ByteBuffer buf = value.getBuffers().next();
			OutputStreams.write(buf.array(), buf.position(), buf.limit(), out);
		}
		OutputStreams.write('\r', out);
		OutputStreams.write('\n', out);
	}
}
