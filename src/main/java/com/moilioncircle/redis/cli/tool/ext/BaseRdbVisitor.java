package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Baoyi Chen
 */
public class BaseRdbVisitor extends DefaultRdbVisitor {

    protected Long db;
    protected Long top;
    protected Escape escape;
    protected Pattern keyRegEx;
    protected List<Type> types;
    protected OutputStream out;

    public BaseRdbVisitor(Replicator replicator, File output, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator);
        this.db = db;
        this.top = top;
        this.types = types;
        this.escape = escape;
        this.keyRegEx = Pattern.compile(keyRegEx);
        this.out = new BufferedOutputStream(new FileOutputStream(output));
        replicator.addCloseListener(r -> {
            try {
                out.close();
            } catch (IOException e) {
            }
        });
    }

    protected boolean contains(int rdbType) {
        return Type.contains(types, rdbType);
    }

    protected boolean contains(long db) {
        return this.db == null || this.db.intValue() == db;
    }

    protected boolean contains(String key) {
        return keyRegEx.matcher(Strings.toString(key)).matches();
    }

    protected boolean contains(long db, int rdbType, String key) {
        return contains(db) && contains(rdbType) && contains(key);
    }

    protected static void escape(Escape escape, double value, OutputStream out) throws IOException {
        escape(escape, String.valueOf(value).getBytes(), out);
    }

    protected static void escape(Escape escape, byte[] bytes, OutputStream out) throws IOException {
        if (bytes == null) return;
        switch (escape) {
            case RAW:
                out.write(bytes);
                break;
            case UTF8:
                out.write(Strings.toString(bytes).getBytes());
                break;
            case PRINT:
                for (byte b : bytes) {
                    if (b == '\n') {
                        out.write('\\');
                        out.write('n');
                    } else if (b == '\r') {
                        out.write('\\');
                        out.write('r');
                    } else if (b == '\t') {
                        out.write('\\');
                        out.write('t');
                    } else if (b == '\b') {
                        out.write('\\');
                        out.write('b');
                    } else if (b == '\f') {
                        out.write('\\');
                        out.write('f');
                    } else if (b == '"') {
                        out.write('\\');
                        out.write('"');
                    } else if (b == 7) {
                        out.write('\\');
                        out.write('a');
                    } else if (!isPrint(b)) {
                        out.write('\\');
                        out.write('x');
                        out.write(Integer.toHexString(b & 0xFf).getBytes());
                    } else {
                        out.write(b);
                    }
                }
                break;
            case BASE64:
                out.write(Base64.getEncoder().encode(bytes));
                break;
        }
    }

    private static boolean isPrint(int i) {
        return (i >= 33 && i <= 126) || (i >= 161 && i <= 255);
    }
}
