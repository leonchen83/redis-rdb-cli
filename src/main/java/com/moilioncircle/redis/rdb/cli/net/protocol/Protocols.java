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

import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.DESCRIPTION_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.FIVE;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.FOUR;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.FUNCTION_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.LOAD_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.REPLACE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.RESTORE_BUF;
import static com.moilioncircle.redis.rdb.cli.ext.datatype.CommandConstants.THREE;
import static com.moilioncircle.redis.replicator.Constants.DOLLAR;
import static com.moilioncircle.redis.replicator.Constants.STAR;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.replicator.rdb.datatype.Function;

/**
 * @author Baoyi Chen
 */
public class Protocols {
	
	public static void emit(OutputStream out, ByteBuffer command, ByteBuffer... ary) {
		Outputs.write(STAR, out);
		Outputs.write(String.valueOf(ary.length + 1).getBytes(), out);
		writeCrLf(out);
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(command.remaining()).getBytes(), out);
		writeCrLf(out);
		Outputs.write(command.array(), command.position(), command.limit(), out);
		writeCrLf(out);
		for (final ByteBuffer arg : ary) {
			Outputs.write(DOLLAR, out);
			Outputs.write(String.valueOf(arg.remaining()).getBytes(), out);
			writeCrLf(out);
			Outputs.write(arg.array(), arg.position(), arg.limit(), out);
			writeCrLf(out);
		}
	}
	
	public static void emit(OutputStream out, ByteBuffers command, ByteBuffers... ary) {
		Outputs.write(STAR, out);
		Outputs.write(String.valueOf(ary.length + 1).getBytes(), out);
		writeCrLf(out);
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(command.getSize()).getBytes(), out);
		writeCrLf(out);
		Iterator<ByteBuffer> cit = command.getBuffers();
		while (cit.hasNext()) {
			ByteBuffer tmp = cit.next();
			Outputs.write(tmp.array(), tmp.position(), tmp.limit(), out);
		}
		writeCrLf(out);
		for (final ByteBuffers arg : ary) {
			Outputs.write(DOLLAR, out);
			Outputs.write(String.valueOf(arg.getSize()).getBytes(), out);
			writeCrLf(out);
			Iterator<ByteBuffer> ait = arg.getBuffers();
			while (ait.hasNext()) {
				ByteBuffer tmp = ait.next();
				Outputs.write(tmp.array(), tmp.position(), tmp.limit(), out);
			}
			writeCrLf(out);
		}
	}
	
	public static void emit(OutputStream out, byte[] command, byte[]... ary) {
		Outputs.write(STAR, out);
		Outputs.write(String.valueOf(ary.length + 1).getBytes(), out);
		writeCrLf(out);
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(command.length).getBytes(), out);
		writeCrLf(out);
		Outputs.write(command, out);
		writeCrLf(out);
		for (final byte[] arg : ary) {
			Outputs.write(DOLLAR, out);
			Outputs.write(String.valueOf(arg.length).getBytes(), out);
			writeCrLf(out);
			Outputs.write(arg, out);
			writeCrLf(out);
		}
	}
	
	public static void emit(OutputStream out, byte[] command, byte[] key, List<byte[]> ary) {
		Outputs.write(STAR, out);
		Outputs.write(String.valueOf(ary.size() + 2).getBytes(), out);
		writeCrLf(out);
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(command.length).getBytes(), out);
		writeCrLf(out);
		Outputs.write(command, out);
		writeCrLf(out);
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(key.length).getBytes(), out);
		writeCrLf(out);
		Outputs.write(key, out);
		writeCrLf(out);
		for (final byte[] arg : ary) {
			Outputs.write(DOLLAR, out);
			Outputs.write(String.valueOf(arg.length).getBytes(), out);
			writeCrLf(out);
			Outputs.write(arg, out);
			writeCrLf(out);
		}
	}
	
	public static void restore(OutputStream out, ByteBuffer key, ByteBuffer ex, ByteBuffers value, boolean replace) {
		Outputs.write(STAR, out);
		if (replace) {
			Outputs.write(FIVE, out);
		} else {
			Outputs.write(FOUR, out);
		}
		writeCrLf(out);
		emitArg(out, RESTORE_BUF);
		emitArg(out, key);
		emitArg(out, ex);
		emitArg(out, value);
		if (replace) {
			emitArg(out, REPLACE_BUF);
		}
	}
	
	public static void functionRestore(OutputStream out, ByteBuffers value, boolean replace) {
		Outputs.write(STAR, out);
		if (replace) {
			Outputs.write(FOUR, out);
		} else {
			Outputs.write(THREE, out);
		}
		writeCrLf(out);
		emitArg(out, FUNCTION_BUF);
		emitArg(out, RESTORE_BUF);
		emitArg(out, value);
		if (replace) {
			emitArg(out, REPLACE_BUF);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void functionLoad(OutputStream out, Function function, boolean replace) {
		if (function.getName() == null) {
			int count = 3;
			if (replace) {
				count += 1;
			}
			Outputs.write(STAR, out);
			Outputs.write(String.valueOf(count).getBytes(), out);
			writeCrLf(out);
			emitArg(out, FUNCTION_BUF);
			emitArg(out, LOAD_BUF);
			if (replace) {
				emitArg(out, REPLACE_BUF);
			}
			emitArg(out, ByteBuffer.wrap(function.getCode()));
		} else {
			int count = 5;
			if (function.getDescription() != null) {
				count += 2;
			}
			if (replace) {
				count += 1;
			}
			Outputs.write(STAR, out);
			Outputs.write(String.valueOf(count).getBytes(), out);
			writeCrLf(out);
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
	}
	
	private static void emitArg(OutputStream out, ByteBuffer arg) {
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(arg.remaining()).getBytes(), out);
		writeCrLf(out);
		Outputs.write(arg.array(), arg.position(), arg.limit(), out);
		writeCrLf(out);
	}
	
	private static void emitArg(OutputStream out, ByteBuffers value) {
		Outputs.write(DOLLAR, out);
		Outputs.write(String.valueOf(value.getSize()).getBytes(), out);
		writeCrLf(out);
		while (value.getBuffers().hasNext()) {
			ByteBuffer buf = value.getBuffers().next();
			Outputs.write(buf.array(), buf.position(), buf.limit(), out);
		}
		writeCrLf(out);
	}
	
	private static void writeCrLf(OutputStream out) {
		Outputs.write('\r', out);
		Outputs.write('\n', out);
	}
}
