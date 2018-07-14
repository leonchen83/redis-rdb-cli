package com.moilioncircle.redis.cli.tool.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
public class BufferedOutputStream extends OutputStream {
    
    protected int count;
    protected byte buf[];
    protected OutputStream out;
    
    public BufferedOutputStream(OutputStream out, int size) {
        this.out = out;
        buf = new byte[size];
    }
    
    public void write(int b) throws IOException {
        if (count >= buf.length) {
            flush();
        }
        buf[count++] = (byte) b;
    }
    
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            int min = Math.min(len, buf.length - count);
            System.arraycopy(b, off, buf, count, min);
            count += min;
            if (count == buf.length) {
                out.write(buf, 0, count);
                out.flush();
                count = 0;
            }
            off += min;
            len -= min;
        }
    }
    
    public void flush() throws IOException {
        if (count > 0) {
            out.write(buf, 0, count);
            out.flush();
            count = 0;
        }
    }
    
    public void close() throws IOException {
        flush();
        out.close();
    }
}
