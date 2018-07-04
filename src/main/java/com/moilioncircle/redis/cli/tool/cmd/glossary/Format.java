package com.moilioncircle.redis.cli.tool.cmd.glossary;

import com.moilioncircle.redis.cli.tool.ext.DumpRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.JsonRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.KeyRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.KeyValRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.MemRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.RespRdbVisitor;
import com.moilioncircle.redis.replicator.Replicator;

import java.io.File;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public enum Format {
    KEY("key"),
    MEM("mem"),
    DUMP("dump"),
    JSON("json"),
    RESP("resp"),
    KEYVAL("keyval");

    private String value;

    Format(String value) {
        this.value = value;
    }

    public static Format parse(String format) {
        return Format.valueOf(format);
    }

    public void dress(Replicator r, File output, List<Long> db, List<String> regexs, Long largest, Long bytes, List<Type> types, Escape escape) throws Exception {
        switch (this) {
            case KEY:
                r.setRdbVisitor(new KeyRdbVisitor(r, output, db, regexs, types, escape));
                break;
            case JSON:
                r.setRdbVisitor(new JsonRdbVisitor(r, output, db, regexs, types, escape));
                break;
            case DUMP:
                r.setRdbVisitor(new DumpRdbVisitor(r, output, db, regexs, types, escape));
                break;
            case RESP:
                r.setRdbVisitor(new RespRdbVisitor(r, output, db, regexs, types, escape));
                break;
            case KEYVAL:
                r.setRdbVisitor(new KeyValRdbVisitor(r, output, db, regexs, types, escape));
                break;
            case MEM:
                r.setRdbVisitor(new MemRdbVisitor(r, output, db, regexs, types, escape, largest, bytes));
                break;
            default:
                throw new AssertionError(this);
        }
    }
}
