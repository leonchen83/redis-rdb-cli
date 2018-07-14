package com.moilioncircle.redis.cli.tool.glossary;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.rct.DiffRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rct.DumpRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rct.JsonRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rct.KeyRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rct.KeyValRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rct.MemRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rct.RespRdbVisitor;
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
    DIFF("diff"),
    JSON("json"),
    RESP("resp"),
    KEYVAL("keyval");

    private String value;

    Format(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static Format parse(String format) {
        switch (format) {
            case "key":
                return KEY;
            case "mem":
                return MEM;
            case "dump":
                return DUMP;
            case "diff":
                return DIFF;
            case "json":
                return JSON;
            case "resp":
                return RESP;
            case "keyval":
                return KEYVAL;
            default:
                throw new AssertionError("Unsupported format '" + format + "'");
        }
    }

    public void dress(Replicator r, Configure conf, File output, List<Long> db, List<String> regexs, Long largest, Long bytes, List<DataType> types, Escape escape) {
        switch (this) {
            case DIFF:
                r.setRdbVisitor(new DiffRdbVisitor(r, conf, output, db, regexs, types));
                break;
            case DUMP:
                r.setRdbVisitor(new DumpRdbVisitor(r, conf, output, db, regexs, types));
                break;
            case RESP:
                r.setRdbVisitor(new RespRdbVisitor(r, conf, output, db, regexs, types));
                break;
            case KEY:
                r.setRdbVisitor(new KeyRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case JSON:
                r.setRdbVisitor(new JsonRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case KEYVAL:
                r.setRdbVisitor(new KeyValRdbVisitor(r, conf, output, db, regexs, types, escape));
                break;
            case MEM:
                r.setRdbVisitor(new MemRdbVisitor(r, conf, output, db, regexs, types, escape, largest, bytes));
                break;
            default:
                throw new AssertionError("Unsupported format '" + this + "'");
        }
    }
}
