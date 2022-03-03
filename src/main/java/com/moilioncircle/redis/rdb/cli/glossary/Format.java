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

import static java.util.ServiceLoader.load;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.api.format.FormatterService;
import com.moilioncircle.redis.rdb.cli.api.format.escape.Escaper;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.escape.Escapers;
import com.moilioncircle.redis.rdb.cli.ext.escape.JsonEscaper;
import com.moilioncircle.redis.rdb.cli.ext.escape.RawEscaper;
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
import com.moilioncircle.redis.replicator.Replicator;

/**
 * @author Baoyi Chen
 */
public class Format {

    private String value;
    private Configure configure;
    private List<FormatterService> formatters = new ArrayList<>();

    public Format(String value, Configure configure) {
        this.value = value;
        this.configure = configure;
        Iterator<FormatterService> it = load(FormatterService.class).iterator();
        while (it.hasNext()) this.formatters.add(it.next());
    }

    public void dress(Replicator r, File output, Filter filter, Long largest, Long bytes, String escaper, boolean replace) {
        // self define formatter has highest priority
        boolean found = false;
        for (FormatterService formatter : formatters) {
            if (formatter.format() == null) continue;
            if (!formatter.format().equals(value)) continue;
            r.setRdbVisitor(new FormatterRdbVisitor(r, configure, output, filter, getEscaper(escaper), formatter));
            found = true;
            break;
        }
        if (found) return;
        
        switch (value) {
            case "diff":
                r.setRdbVisitor(new DiffRdbVisitor(r, configure, output, filter, getEscaper("redis")));
                break;
            case "count":
                r.setRdbVisitor(new CountRdbVisitor(r, configure, output, filter, getEscaper("redis")));
                break;
            case "dump":
                r.setRdbVisitor(new DumpRdbVisitor(r, configure, output, filter, replace, getEscaper("raw")));
                break;
            case "resp":
                r.setRdbVisitor(new RespRdbVisitor(r, configure, output, filter, replace, getEscaper("raw")));
                break;
            case "key":
                r.setRdbVisitor(new KeyRdbVisitor(r, configure, output, filter, getEscaper(escaper)));
                break;
            case "keyval":
                r.setRdbVisitor(new KeyValRdbVisitor(r, configure, output, filter, getEscaper(escaper)));
                break;
            case "mem":
                r.setRdbVisitor(new MemRdbVisitor(r, configure, output, filter, getEscaper(escaper), largest, bytes));
                break;
            case "json":
                r.setRdbVisitor(new JsonRdbVisitor(r, configure, output, filter, getEscaper(escaper, new JsonEscaper())));
                break;
            case "jsonl":
                r.setRdbVisitor(new JsonlRdbVisitor(r, configure, output, filter, getEscaper(escaper, new JsonEscaper())));
                break;
            default:
                throw new AssertionError("Unsupported format '" + value + "'");
        }
    }

    private Escaper getEscaper(String escaper, Escaper defaultValue) {
        return Escapers.parse(escaper, defaultValue, configure.getDelimiter(), configure.getQuote());
    }

    private Escaper getEscaper(String escaper) {
        return Escapers.parse(escaper, new RawEscaper(), configure.getDelimiter(), configure.getQuote());
    }
}
