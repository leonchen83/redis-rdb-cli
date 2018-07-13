package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.io.CRCOutputStream;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
public class DumpRawByteListener implements RawByteListener, Closeable {
    private final int version;
    private final CRCOutputStream out;
    
    public DumpRawByteListener(byte type, int version, OutputStream out, Escape escape, Configure conf) throws IOException {
        this.version = version;
        this.out = new CRCOutputStream(out, escape, conf);
        this.out.write(type);
    }
    
    @Override
    public void handle(byte... rawBytes) {
        OutputStreams.writeQuietly(rawBytes, out);
    }
    
    @Override
    public void close() throws IOException {
        this.out.write((byte) version);
        this.out.write((byte) 0x00);
        this.out.write(this.out.getCRC64());
    }
}