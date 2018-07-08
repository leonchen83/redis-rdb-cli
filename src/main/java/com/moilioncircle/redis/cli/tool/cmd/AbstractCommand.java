package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisURI;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import static com.moilioncircle.redis.cli.tool.cmd.Version.VERSION;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractCommand implements Command {
    
    protected Options options = new Options();
    
    protected abstract void doExecute(CommandLine line) throws Exception;
    
    @Override
    public void addOption(Option option) {
        options.addOption(option);
    }
    
    @Override
    public void execute(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        try {
            org.apache.commons.cli.CommandLine line = parser.parse(options, args);
            doExecute(new CommandLine(line));
        } catch (Exception e) {
            e.printStackTrace();
            if (e.getMessage() != null) {
                writeLine(e.getMessage());
            } else {
                throw e;
            }
        }
    }
    
    protected String normalize(String source, FileType type, String message) throws URISyntaxException {
        RedisURI uri;
        try {
            uri = new RedisURI(source);
        } catch (Throwable e) {
            URI u = new File(source).toURI();
            uri = new RedisURI(new URI("redis", u.getRawAuthority(), u.getRawPath(), u.getRawQuery(), u.getRawFragment()).toString());
        }
        if (uri != null && (uri.getFileType() == null || type == null || uri.getFileType() == type)) {
            return source;
        }
        throw new AssertionError(message);
    }
    
    protected void write(String message) throws Exception {
        System.out.print(message);
        System.out.flush();
    }
    
    protected void writeLine(String message) throws Exception {
        System.out.println(message);
    }
    
    protected String version() {
        StringBuilder builder = new StringBuilder();
        builder.append("redis cli tool: ").append(VERSION).append("\n");
        builder.append("java version: ").append(System.getProperty("java.version")).append(", ");
        builder.append("vendor: ").append(System.getProperty("java.vendor")).append("\n");
        builder.append("java home: ").append(System.getProperty("java.home")).append("\n");
        builder.append("default locale: ").append(System.getProperty("user.language")).append(", ");
        builder.append("platform encoding: ").append(System.getProperty("file.encoding")).append("\n");
        builder.append("os name: ").append(System.getProperty("os.name")).append(", ");
        builder.append("version: ").append(System.getProperty("os.version")).append(", ");
        builder.append("arch: ").append(System.getProperty("os.arch"));
        return builder.toString();
    }
}
