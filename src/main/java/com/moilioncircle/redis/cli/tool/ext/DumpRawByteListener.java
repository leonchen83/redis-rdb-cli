package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.io.CRCOutputStream;
import com.moilioncircle.redis.cli.tool.util.OutputStreams;
import com.moilioncircle.redis.replicator.cmd.RedisCodec;
import com.moilioncircle.redis.replicator.io.RawByteListener;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static com.moilioncircle.redis.replicator.util.CRC64.crc64;
import static com.moilioncircle.redis.replicator.util.CRC64.longToByteArray;

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
    
    public static void main(String[] args) throws IOException {
        String str = "\\x005this is a string value617442 with escape character \\r\\n\\b\\x00y\\xb8\\x10\\xe6\\xdb\\x97(\\\\";
        RedisCodec codec = new RedisCodec();
        byte[] bytes = codec.decode(str.getBytes());
        System.out.println(new String(codec.encode(bytes)));
        long s = crc64(bytes, 0, bytes.length - 8);
        byte[] b = longToByteArray(s);
        System.out.println(new String(Escape.REDIS.encode(b, Configure.bind())));
        System.out.println(new String(codec.encode(b)));
        
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (DumpRawByteListener listener = new DumpRawByteListener((byte) 0, 8, out, Escape.REDIS, Configure.bind())) {
                for (int i = 1; i < bytes.length - 10; i++) {
                    listener.handle(bytes[i]);
                }
            }
            System.out.println(new String(out.toByteArray()));
        }
        
        
    }
}