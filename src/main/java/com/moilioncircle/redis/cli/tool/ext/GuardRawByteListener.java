package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.glossary.Guard;
import com.moilioncircle.redis.cli.tool.util.io.GuardOutputStream;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import java.io.IOException;
import java.io.OutputStream;

import static com.moilioncircle.redis.replicator.util.CRC64.crc64;
import static com.moilioncircle.redis.replicator.util.CRC64.longToByteArray;

/**
 * @author Baoyi Chen
 */
public class GuardRawByteListener implements RawByteListener {
    private int version;
    private GuardOutputStream out;
    private OutputStream internal;
    
    public GuardRawByteListener(OutputStream internal) {
        this.internal = internal;
        this.out = new GuardOutputStream(8192, internal);
    }
    
    public <T extends OutputStream> T getInternal() {
        return (T) this.internal;
    }
    
    public GuardRawByteListener(byte type, int version, OutputStream internal) throws IOException {
        this.internal = internal;
        this.out = new GuardOutputStream(8192, internal);
        this.out.write((int) type);
        this.version = version;
    }
    
    public void reset(OutputStream out) {
        this.internal = out;
        this.out.reset(out);
    }
    
    public void setGuard(Guard guard) {
        this.out.setGuard(guard);
    }
    
    @Override
    public void handle(byte... rawBytes) {
        try {
            this.out.write(rawBytes);
        } catch (IOException e) {
        }
    }
    
    public byte[] getBytes() throws IOException {
        this.out.write((byte) version);
        this.out.write((byte) 0x00);
        byte[] bytes = this.out.array();
        byte[] crc = longToByteArray(crc64(bytes));
        this.out.write(crc);
        return this.out.array();
    }
}