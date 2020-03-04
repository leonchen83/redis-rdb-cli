package com.moilioncircle.redis.rdb.cli.glossary;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;

import org.junit.Test;

/**
 * @author Baoyi Chen
 */
public class JsonEscapeTest {
	@Test
	public void testEncode() {
		String s = "a\b\f\r\n/\t\rb\u001a中国";
		JsonEscape escape = new JsonEscape(Escape.RAW);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		escape.encode(s.getBytes(), out, null);
		assertEquals("a\\b\\f\\r\\n\\/\\t\\rb\\u001A中国", new String(out.toByteArray()));
	}
}