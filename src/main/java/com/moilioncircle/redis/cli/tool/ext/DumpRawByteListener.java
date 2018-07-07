package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
public class DumpRawByteListener implements RawByteListener, Closeable {
    private final int version;
    private final CRCOutputStream sub;

    public DumpRawByteListener(byte type, int version, OutputStream out, Escape escape) throws IOException {
        this.version = version;
        this.sub = new CRCOutputStream(out, escape);
        sub.write(type);
    }

    @Override
    public void handle(byte... rawBytes) {
        try {
            sub.write(rawBytes);
        } catch (IOException e) {
        }
    }

    @Override
    public void close() throws IOException {
        this.sub.write((byte) version);
        this.sub.write((byte) 0x00);
        this.sub.write(this.sub.getCRC64());
    }
}