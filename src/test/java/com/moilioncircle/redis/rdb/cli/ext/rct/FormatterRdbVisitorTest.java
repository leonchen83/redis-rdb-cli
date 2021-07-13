package com.moilioncircle.redis.rdb.cli.ext.rct;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.nio.file.Files;

import org.junit.Before;
import org.junit.Test;

import com.moilioncircle.redis.rdb.cli.cmd.XRct;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
public class FormatterRdbVisitorTest {

    private String path;

    private String path(String path) {
        return this.path + "/" + path;
    }

    @Before
    public void before() {
        // avoid log4j exception
        ClassLoader loader = FormatterRdbVisitorTest.class.getClassLoader();
        String source = loader.getResource("log4j2.xml").getPath();
        this.path = new File(source).getParent();
        System.setProperty("cli.log.path", path);
    }

    @Test
    public void test() throws Exception {
        String source = path("dumpV8.rdb");
        String target = path("dumpV8-result.txt");
        
        // rct -f test -s ./dumpV8.rdb -o dumpV8-result.txt -e json
        new CommandLine(new XRct()).execute(new String[]{"-f", "test", "-s", source, "-o", target, "-e", "json"});
        assertArrayEquals(expect(), Files.readAllBytes(new File(target).toPath()));
    }
    
    private byte[] expect() {
        String v = "key2,there\n" +
                "key1,Hello\n" +
                "mset1,hello\n" +
                "set,value\n" +
                "bitfield,\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0001\\u0000\n" +
                "bitopdest,`bc\\u0000\\u0000\\u0000\n" +
                "append,value\n" +
                "esss,value\n" +
                "bitop2,abcdef\n" +
                "decrby,5\n" +
                "mset2,world\n" +
                "exxx,value\n" +
                "getset,0\n" +
                "mykey3,Hello\n" +
                "incrby,16\n" +
                "bitop1,foo\n";
        return v.getBytes();
    }
}