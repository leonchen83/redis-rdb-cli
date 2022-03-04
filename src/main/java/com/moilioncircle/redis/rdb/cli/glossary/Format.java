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

package com.moilioncircle.redis.rdb.cli.glossary;

import static com.moilioncircle.redis.rdb.cli.ext.escape.Escapers.getEscaper;

import java.io.File;
import java.util.ServiceLoader;
import java.util.function.Predicate;

import com.moilioncircle.redis.rdb.cli.api.format.FormatterService;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.escape.JsonEscaper;
import com.moilioncircle.redis.rdb.cli.ext.rct.CountRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.DiffRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.DumpRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.FormatterRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.JsonRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.JsonlRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.KeyRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.KeyValRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.MemRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rct.RespRdbVisitor;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.rdb.cli.util.Iterators;
import com.moilioncircle.redis.replicator.Replicator;

/**
 * @author Baoyi Chen
 */
public class Format {

    private String value;
    private Configure configure;
    private FormatterService formatter;

    public Format(String value, Configure configure) {
        this.value = value;
        this.configure = configure;
        Predicate<FormatterService> test = e -> e.format() != null && e.format().equals(value);
        this.formatter = Iterators.find(ServiceLoader.load(FormatterService.class).iterator(), test);
    }

    public void dress(Replicator r, Filter filter, File output, Long largest, Long bytes, String escaper, boolean replace) {
        // self define formatter has highest priority
        if (formatter != null) {
            r.setRdbVisitor(new FormatterRdbVisitor(r, configure, filter, output, getEscaper(escaper, configure), formatter));
            return;
        }
        
        switch (value) {
            case "diff":
                r.setRdbVisitor(new DiffRdbVisitor(r, configure, filter, output, getEscaper("redis", configure)));
                break;
            case "count":
                r.setRdbVisitor(new CountRdbVisitor(r, configure, filter, output, getEscaper("redis", configure)));
                break;
            case "dump":
                r.setRdbVisitor(new DumpRdbVisitor(r, configure, filter, output, replace, getEscaper("raw", configure)));
                break;
            case "resp":
                r.setRdbVisitor(new RespRdbVisitor(r, configure, filter, output, replace, getEscaper("raw", configure)));
                break;
            case "key":
                r.setRdbVisitor(new KeyRdbVisitor(r, configure, filter, output, getEscaper(escaper, configure)));
                break;
            case "keyval":
                r.setRdbVisitor(new KeyValRdbVisitor(r, configure, filter, output, getEscaper(escaper, configure)));
                break;
            case "mem":
                r.setRdbVisitor(new MemRdbVisitor(r, configure, filter, output, getEscaper(escaper, configure), largest, bytes));
                break;
            case "json":
                r.setRdbVisitor(new JsonRdbVisitor(r, configure, filter, output, getEscaper(escaper, new JsonEscaper(), configure)));
                break;
            case "jsonl":
                r.setRdbVisitor(new JsonlRdbVisitor(r, configure, filter, output, getEscaper(escaper, new JsonEscaper(), configure)));
                break;
            default:
                throw new AssertionError("Unsupported format '" + value + "'");
        }
    }
}
