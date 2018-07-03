package com.moilioncircle.redis.cli.tool.util.io;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;

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

    public CRCOutputStream(OutputStream out, Escape escape) {
        this.out = out;
        this.escape = escape;
    }

    public byte[] getCRC64() {
        return longToByteArray(checksum);
    }

    @Override
    public void write(int b) throws IOException {
        escape.encode(b, out);
        checksum = crc64(new byte[]{(byte) b}, checksum);
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        escape.encode(b, off, len, out);
        checksum = crc64(b, off, len, checksum);
    }

    public void flush() throws IOException {
        out.flush();
    }

    public void close() throws IOException {
        out.close();
    }
}