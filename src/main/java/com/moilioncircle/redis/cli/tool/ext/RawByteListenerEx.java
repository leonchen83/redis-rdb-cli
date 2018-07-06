package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.util.ByteBuilder;

import static com.moilioncircle.redis.replicator.util.CRC64.crc64;
import static com.moilioncircle.redis.replicator.util.CRC64.longToByteArray;

/**
 * @author Baoyi Chen
 */
public class RawByteListenerEx implements RawByteListener {
    private final int version;
    private final ByteBuilder builder;

    public RawByteListenerEx(byte type, int version) {
        this.builder = ByteBuilder.allocate(8192);
        this.builder.put(type);
        this.version = version;
    }

    @Override
    public void handle(byte... rawBytes) {
        for (byte b : rawBytes) this.builder.put(b);
    }

    public byte[] getBytes() {
        this.builder.put((byte) version);
        this.builder.put((byte) 0x00);
        byte[] bytes = this.builder.array();
        byte[] crc = longToByteArray(crc64(bytes));
        for (byte b : crc) {
            this.builder.put(b);
        }
        return this.builder.array();
    }
}