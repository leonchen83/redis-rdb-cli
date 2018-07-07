package com.moilioncircle.redis.cli.tool.util.io;

import com.moilioncircle.redis.cli.tool.util.NodeConf;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.moilioncircle.redis.cli.tool.util.CRC16.crc16;

/**
 * @author Baoyi Chen
 */
public class FilesOutputStream extends OutputStream {
    
    private byte[] key;
    
    private Set<CRCOutputStream> set = new HashSet<>();
    private Map<Short, CRCOutputStream> map = new HashMap<>();
    
    public FilesOutputStream(String path, String conf) {
        NodeConf.parse(path, conf, set, map);
    }
    
    public void shard(byte[] key) {
        this.key = key;
    }
    
    private short slot(byte[] key) {
        if (key == null) return 0;
        int st = -1, ed = -1;
        for (int i = 0, len = key.length; i < len; i++) {
            if (key[i] == '{' && st == -1) st = i;
            if (key[i] == '}' && st >= 0) {
                ed = i;
                break;
            }
        }
        if (st >= 0 && ed >= 0 && ed > st + 1)
            return (short) (crc16(key, st + 1, ed) & 16383);
        return (short) (crc16(key) & 16383);
    }
    
    @Override
    public void write(int b) throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.write(b);
            }
        } else {
            map.get(slot(key)).write(b);
        }
    }
    
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.write(b);
            }
        } else {
            map.get(slot(key)).write(b);
        }
    }
    
    public void flush() throws IOException {
        if (key == null) {
            for (OutputStream out : set) {
                out.flush();
            }
        } else {
            map.get(slot(key)).flush();
        }
    }
    
    public void close() throws IOException {
        for (OutputStream out : set) {
            out.close();
        }
    }
    
    public void writeCRC() {
        try {
            for (CRCOutputStream out : set) {
                out.write(255); // eof
                byte[] bytes = out.getCRC64();
                out.write(bytes);
            }
        } catch (IOException e) {
        }
    }
}
