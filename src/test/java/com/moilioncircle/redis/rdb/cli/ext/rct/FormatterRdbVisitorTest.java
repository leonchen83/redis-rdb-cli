package com.moilioncircle.redis.rdb.cli.ext.rct;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.nio.file.Files;

import org.junit.Before;
import org.junit.Test;

import com.moilioncircle.redis.rdb.cli.cmd.RctCommand;

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
        String expect = path("dumpV8-expect.txt");
        
        // rct -f test -s ./dumpV8.rdb -o dumpV8-result.txt -e json
        RctCommand.run(new String[]{"-f", "test", "-s", source, "-o", target, "-e", "json"});
        assertArrayEquals(Files.readAllBytes(new File(expect).toPath()), Files.readAllBytes(new File(target).toPath()));
    }
}