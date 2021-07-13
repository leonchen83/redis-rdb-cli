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

package com.moilioncircle.redis.rdb.cli;

import java.util.Arrays;

/**
 * @author Baoyi Chen
 */
public class Main {
	public static void main(String[] args) {
		if (args.length == 0) {
			return;
		}
		String command = args[0];
		if (command == null) {
			return;
		}
		String[] ary = Arrays.copyOfRange(args, 1, args.length);
		if (command.equals("rct")) {
			Rct.main(ary);
		} else if (command.equals("rdt")) {
			Rdt.main(ary);
		} else if (command.equals("ret")) {
			Ret.main(ary);
		} else if (command.equals("rmt")) {
			Rmt.main(ary);
		} else if (command.equals("rst")) {
			Rst.main(ary);
		}
	}
}
