package com.moilioncircle.redis.rdb.cli.conf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

/**
 * @author Baoyi Chen
 */
public class NodeConfParserTest {
	
	@Test
	public void test0() throws Exception {
		String conf = readConf("rdt/nodes.conf");
		XClusterNodes nodes = NodeConfParser.parse(conf);
		assertEquals(nodes.getNodes().get(0).getHostAndPort().getHost(), "127.0.0.1");
		assertEquals(nodes.getNodes().get(1).getHostAndPort().getHost(), "127.0.0.1");
		assertEquals(nodes.getNodes().get(2).getHostAndPort().getHost(), "127.0.0.1");
		assertEquals(nodes.getNodes().get(3).getHostAndPort().getHost(), "127.0.0.1");
		assertEquals(nodes.getNodes().get(4).getHostAndPort().getHost(), "127.0.0.1");
		assertEquals(nodes.getNodes().get(5).getHostAndPort().getHost(), "127.0.0.1");
	}
	
	@Test
	public void test1() throws Exception {
		String conf = readConf("rdt/nodes-hostname.conf");
		XClusterNodes nodes = NodeConfParser.parse(conf);
		assertEquals(nodes.getNodes().get(0).getHostAndPort().getHost(), "redis-host1");
		assertEquals(nodes.getNodes().get(1).getHostAndPort().getHost(), "redis-host2");
		assertEquals(nodes.getNodes().get(2).getHostAndPort().getHost(), "redis-host3");
		assertEquals(nodes.getNodes().get(3).getHostAndPort().getHost(), "redis-host4");
		assertEquals(nodes.getNodes().get(4).getHostAndPort().getHost(), "redis-host5");
		assertEquals(nodes.getNodes().get(5).getHostAndPort().getHost(), "redis-host6");
	}
	
	private static String readConf(String path) throws IOException, URISyntaxException {
		return new String(Files.readAllBytes(Paths.get(NodeConfParserTest.class.getClassLoader().getResource(path).toURI())), StandardCharsets.UTF_8);
	}
}