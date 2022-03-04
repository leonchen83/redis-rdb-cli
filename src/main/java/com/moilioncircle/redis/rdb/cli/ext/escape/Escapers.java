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


import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;

/**
 * @author Baoyi Chen
 */
public class Escapers {
    
    public static Escaper parse(String escaper, Escaper defaultValue, byte... excludes) {
        if (escaper == null) return defaultValue;
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
    
    public static Escaper getEscaper(String escaper, Escaper defaultValue, Configure configure) {
        return Escapers.parse(escaper, defaultValue, configure.getDelimiter(), configure.getQuote());
    }
    
    public static Escaper getEscaper(String escaper, Configure configure) {
        return Escapers.parse(escaper, new RawEscaper(), configure.getDelimiter(), configure.getQuote());
    }
}
