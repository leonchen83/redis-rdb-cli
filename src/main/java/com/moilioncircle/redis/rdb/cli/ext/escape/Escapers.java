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

package com.moilioncircle.redis.rdb.cli.ext.escape;


import java.util.function.Supplier;

import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;

/**
 * @author Baoyi Chen
 */
public class Escapers {
    public static Escaper parse(String escaper, Supplier<Escaper> defaultValue, byte... excludes) {
        if (escaper == null) return defaultValue.get();
        switch (escaper) {
            case "raw":
                return new RawEscaper();
            case "json":
                return new JsonEscaper();
            case "redis":
                return new RedisEscaper(excludes);
            default:
                throw new AssertionError("Unsupported escape '" + escaper + "'");
        }
    }
}
