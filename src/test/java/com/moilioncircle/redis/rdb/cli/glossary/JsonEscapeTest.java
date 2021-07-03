package com.moilioncircle.redis.rdb.cli.glossary;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;

import org.junit.Test;

import com.moilioncircle.redis.rdb.cli.ext.escape.JsonEscaper;

/**
 * @author Baoyi Chen
 */
public class JsonEscapeTest {
    @Test
    public void testEncode() {
        String s = "a\b\f\r\n/\t\rb\u001a中国";
        JsonEscaper escape = new JsonEscaper();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        escape.encode(s.getBytes(), out);
        assertEquals("a\\b\\f\\r\\n\\/\\t\\rb\\u001A中国", new String(out.toByteArray()));
    }
}