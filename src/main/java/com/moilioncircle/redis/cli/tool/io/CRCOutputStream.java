package com.moilioncircle.redis.cli.tool.io;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.glossary.Escape;

import java.io.IOException;
import java.io.OutputStream;

import static com.moilioncircle.redis.replicator.util.CRC64.crc64;
import static com.moilioncircle.redis.replicator.util.CRC64.longToByteArray;

/**
 * @author Baoyi Chen
 */
public class CRCOutputStream extends OutputStream {

    private long checksum = 0L;
    private final Escape escape;
    private final OutputStream out;
    private final Configure configure;

    public CRCOutputStream(OutputStream out, Escape escape, Configure configure) {
        this.out = out;
        this.escape = escape;
        this.configure = configure;
    }

    public byte[] getCRC64() {
        return longToByteArray(checksum);
    }

    @Override
    public void write(int b) throws IOException {
        escape.encode(b & 0xFF, out, configure);
        checksum = crc64(new byte[]{(byte) b}, checksum);
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        escape.encode(b, off, len, out, configure);
        checksum = crc64(b, off, len, checksum);
    }

    public void flush() throws IOException {
        out.flush();
    }

    public void close() throws IOException {
        out.close();
    }
}