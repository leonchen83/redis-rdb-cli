package com.moilioncircle.redis.cli.tool.glossary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Baoyi Chen
 */
public enum Escape {
    RAW("raw"),
    REDIS("redis");

    private String value;

    Escape(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static Escape parse(String escape) {
        if (escape == null) return RAW;
        switch (escape) {
            case "raw":
                return RAW;
            case "redis":
                return REDIS;
            default:
                throw new AssertionError("Unsupported escape '" + escape + "'");
        }
    }

    public void encode(double value, OutputStream out) throws IOException {
        encode(String.valueOf(value).getBytes(), out);
    }

    public void encode(long value, OutputStream out) throws IOException {
        encode(String.valueOf(value).getBytes(), out);
    }

    public void encode(int b, OutputStream out) throws IOException {
        switch (this) {
            case RAW:
                out.write(b);
                break;
            case REDIS:
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
                } else if (b == 7) {
                    out.write('\\');
                    out.write('a');
                } else if (b == '"') {
                    out.write('\\');
                    out.write('"');
                } else if (b == '\'') {
                    out.write('\\');
                    out.write('\'');
                } else if (b == '\\') {
                    out.write('\\');
                    out.write('\\');
                } else if (!((b >= 33 && b <= 126) || (b >= 161 && b <= 255))) {
                    out.write('\\');
                    out.write('x');
                    out.write(Integer.toHexString(b & 0xFF).getBytes());
                } else {
                    out.write(b);
                }
                break;
        }
    }

    public void encode(byte[] bytes, int off, int len, OutputStream out) throws IOException {
        if (bytes == null) return;
        switch (this) {
            case RAW:
                out.write(bytes, off, len);
                break;
            case REDIS:
                for (int i = off; i < len; i++) {
                    encode(bytes[i], out);
                }
                break;
            default:
                throw new AssertionError(this);
        }
    }

    public void encode(byte[] bytes, OutputStream out) throws IOException {
        encode(bytes, 0, bytes.length, out);
    }

    public byte[] encode(byte[] bytes) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length)) {
            encode(bytes, 0, bytes.length, out);
            return out.toByteArray();
        }
    }
}
