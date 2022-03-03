package com.moilioncircle.redis.rdb.cli.io;

import static java.nio.ByteBuffer.allocate;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.Test;

import com.moilioncircle.redis.rdb.cli.util.ByteBuffers;

/**
 * @author Baoyi Chen
 */
public class LayeredOutputStreamTest {
	
	@Test
	public void test() throws IOException {
		System.out.println(System.getProperty("java.io.tmpdir"));
		LayeredOutputStream out = new LayeredOutputStream(8, 16);
		String s = "0123456789abcdefghijklmnopqrstuvwxzy";
		out.write(s.getBytes());
		ByteBuffers buffers = out.toByteBuffers();
		{
			Iterator<ByteBuffer> it = buffers.getBuffers();
			ByteBuffer x = allocate(100);
			while (it.hasNext()) {
				ByteBuffer buf = it.next();
				while (buf.hasRemaining()) {
					x.put(buf.get());
				}
			}
			
			x.flip();
			assertEquals(s, new String(x.array(), x.position(), x.limit()));
		}
		
		buffers.reset();
		{
			Iterator<ByteBuffer> it = buffers.getBuffers();
			ByteBuffer x = allocate(100);
			while (it.hasNext()) {
				ByteBuffer buf = it.next();
				while (buf.hasRemaining()) {
					x.put(buf.get());
				}
			}
			
			x.flip();
			assertEquals(s, new String(x.array(), x.position(), x.limit()));
		}
		buffers.close();
	}
	
}