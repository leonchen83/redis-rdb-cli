package com.moilioncircle.redis.rdb.cli.ext;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moilioncircle.redis.rdb.cli.cmd.XRct;
import com.moilioncircle.redis.rdb.cli.cmd.XRdt;
import com.moilioncircle.redis.rdb.cli.cmd.XRmt;

import picocli.CommandLine;

/**
 * @author Baoyi Chen
 */
public class TestCases {

    private String path;
    private static final String ACTUAL_SUFFIX = ".actual";

    private String path(String path) {
        return this.path + "/" + path;
    }

    @Before
    public void before() {
        // avoid log4j exception
        ClassLoader loader = TestCases.class.getClassLoader();
        String source = loader.getResource("log4j2.xml").getPath();
        this.path = new File(source).getParent();
        System.setProperty("cli.log.path", path);
    }
    
    @After
    public void after() {
        System.setProperty("dump_rdb_version", "-1");
    }
    
    private String target(String source, String extension, String suffix) {
        StringBuilder builder = new StringBuilder();
        builder.append(source);
        if (extension != null) {
            builder.append(".").append(extension);
        }
        if (suffix != null) {
            builder.append(suffix);
        }
        return builder.toString();
    }
    
    private String target(String source, String extension) {
        return target(source, extension, null);
    }
    
    private String target(String source) {
        return target(source, null, null);
    }
    
    private void eq1(String source, String target) {
        try {
            byte[] expect = Files.readAllBytes(new File(source).toPath());
            byte[] actual = Files.readAllBytes(new File(target).toPath());
            assertArrayEquals(MessageFormat.format("expect file: {0}, actual file: {1}", source, target), expect, actual);
        } catch (IOException e) {
            fail();
        }
    }
    
    private void eq0(String source, String extension) {
        try {
            String efile = target(source, extension);
            String afile = target(source, extension, ACTUAL_SUFFIX);
            byte[] expect = Files.readAllBytes(new File(efile).toPath());
            byte[] actual = Files.readAllBytes(new File(afile).toPath());
            assertArrayEquals(MessageFormat.format("expect file: {0}, actual file: {1}", efile, afile), expect, actual);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testRct() throws Exception {
        String rct = "rct";
        Files.list(new File(this.path + "/" + rct).toPath()).forEach(e -> {
            String extension = com.google.common.io.Files.getFileExtension(e.getFileName().toString());
            if (extension.equals("rdb")) {
                String source = e.toAbsolutePath().toString();
                new CommandLine(new XRct()).execute(new String[]{"-f", "test", "-s", source, "-o", target(source, "text", ACTUAL_SUFFIX), "-e", "json"});
                new CommandLine(new XRct()).execute(new String[]{"-f", "json", "-s", source, "-o", target(source, "json", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "jsonl", "-s", source, "-o", target(source, "jsonl", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "key", "-s", source, "-o", target(source, "key", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "count", "-s", source, "-o", target(source, "count", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "keyval", "-s", source, "-o", target(source, "keyval", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "resp", "-s", source, "-o", target(source, "resp", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "dump", "-s", source, "-o", target(source, "dump", ACTUAL_SUFFIX)});
                new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", source, "-o", target(source, "diff", ACTUAL_SUFFIX)});
                System.setProperty("dump_rdb_version", "7");
                new CommandLine(new XRct()).execute(new String[]{"-f", "dump", "-s", source, "-o", target(source, "dump7", ACTUAL_SUFFIX)});
                System.setProperty("dump_rdb_version", "-1");
                eq0(source, "text");
                eq0(source, "json");
                eq0(source, "jsonl");
                eq0(source, "key");
                eq0(source, "count");
                eq0(source, "keyval");
                eq0(source, "resp");
                eq0(source, "dump");
                eq0(source, "diff");
                eq0(source, "dump7");
            }
        });
    }
    
    @Test
    public void testRdt() throws Exception {
        String rdt = "rdt";
        Path source = new File(this.path + "/" + rdt).toPath();
        Map<String, String> map = Files.list(source).collect(Collectors.toMap(e -> e.getFileName().toString(), e -> e.toAbsolutePath().toString()));
        System.out.println(map);
        String d1 = map.get("12.rdb");
        String d2 = map.get("34.rdb");
        String d3 = map.get("89.rdb");
        String dump = map.get("dump.rdb");
        String conf = map.get("nodes.conf");
        String target = target(dump, "actual");
        
        new CommandLine(new XRdt()).execute(new String[]{"-m", d1, d2, d3, "-o", target});
        eq1(dump, target);
        new CommandLine(new XRdt()).execute(new String[]{"-s", dump, "-c", conf, "-o", source.toAbsolutePath().toString()});
        map = Files.list(source).collect(Collectors.toMap(e -> e.getFileName().toString(), e -> e.toAbsolutePath().toString()));
        
        String dd1 = map.get("126be0dd065a87eea5cb4036d1d4ee958c02078a.rdb");
        String dd2 = map.get("34b6e1dfb871ad30398ef5edd6b9a954617e6ec1.rdb");
        String dd3 = map.get("89d020a7e727e81f003836207902ae26fe05fd51.rdb");
        
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", d1, "-o", target(d1, "diff")});
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", dd1, "-o", target(dd1, "diff")});
        eq1(target(d1, "diff"), target(dd1, "diff"));
    
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", d2, "-o", target(d2, "diff")});
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", dd2, "-o", target(dd2, "diff")});
        eq1(target(d2, "diff"), target(dd2, "diff"));
    
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", d3, "-o", target(d3, "diff")});
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", dd3, "-o", target(dd3, "diff")});
        eq1(target(d3, "diff"), target(dd3, "diff"));
    
        new CommandLine(new XRdt()).execute(new String[]{"-b", dump, "-t", "string", "-o", target(dump, "rdb")});
        map = Files.list(source).collect(Collectors.toMap(e -> e.getFileName().toString(), e -> e.toAbsolutePath().toString()));
        String actualString = map.get("dump.rdb.rdb");
        String string = map.get("string.rdb");
    
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", string, "-o", target(string, "diff")});
        new CommandLine(new XRct()).execute(new String[]{"-f", "diff", "-s", actualString, "-o", target(actualString, "diff")});
        eq1(target(string, "diff"), target(actualString, "diff"));
    }
    
    @Test
    public void testRmt() throws Exception {
        String rmt = "rmt";
        Path source = new File(this.path + "/" + rmt).toPath();
        Map<String, String> map = Files.list(source).collect(Collectors.toMap(e -> e.getFileName().toString(), e -> e.toAbsolutePath().toString()));
        String dump = map.get("dump.rdb");
        System.setProperty("dump_rdb_version", "7");
        new CommandLine(new XRmt()).execute(new String[]{"-s", dump, "-m", "redis://127.0.0.1:6379", "-r" , "-t", "sortedset", "list", "hash", "string", "set"});
        new CommandLine(new XRct()).execute(new String[]{"-f", "count", "-s", "redis://127.0.0.1:6379", "-o", target(dump, "count")});
    
        new CommandLine(new XRmt()).execute(new String[]{"-s", "redis://127.0.0.1:6379", "-m", "redis://127.0.0.1:6380?authPassword=test", "-r" , "-t", "sortedset", "list", "hash", "string", "set"});
        new CommandLine(new XRct()).execute(new String[]{"-f", "count", "-s", "redis://127.0.0.1:6380?authPassword=test", "-o", target(dump, "count", ACTUAL_SUFFIX)});
        eq0(dump, "count");
    }
}