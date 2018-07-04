package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.ext.MigrateRdbVisitor;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;
import java.net.URI;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class RmtCommand extends AbstractCommand {
    
    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rmt usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rmt version.").build();
    private static final Option SOURCE = Option.builder("s").longOpt("source").required(false).hasArg().argName("uri").type(String.class).desc("Source uri. eg: redis://host:port?authPassword=foobar redis:///path/to/dump.rdb redis:///path/to/appendonly.aof.").build();
    private static final Option INPUT = Option.builder("i").longOpt("in").required(false).hasArg().argName("file").type(File.class).desc("Input file.").build();
    private static final Option REPLACE = Option.builder("r").longOpt("replace").required(false).desc("Replace exist key value. If not specified default value is false.").build();
    private static final Option MIGRATE = Option.builder("m").longOpt("migrate").required(false).hasArg().argName("uri").type(String.class).desc("Migrate to uri. eg: redis://host:port?authPassword=foobar.").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("Database Number. Multiple databases can be provided. If not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex regex...").valueSeparator(' ').desc("Keys to export. This can be a RegEx.").build();
    private static final Option TYPE = Option.builder("t").longOpt("type").required(false).hasArgs().argName("type type...").valueSeparator(' ').desc("Data type to include. Possible values are string, hash, set, sortedset, list, module(--format [mem|dump|key]), stream(--format [mem|dump|key]). Multiple types can be provided. If not specified, all data types will be returned.").build();
    
    public RmtCommand() {
        addOption(HELP);
        addOption(VERSION);
        addOption(SOURCE);
        addOption(INPUT);
        addOption(REPLACE);
        addOption(MIGRATE);
        addOption(DB);
        addOption(KEY);
        addOption(TYPE);
    }
    
    @Override
    protected void doExecute(CommandLine line) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("rmt", options);
        } else if (line.hasOption("version")) {
            writeLine(version());
        } else {
            StringBuilder sb = new StringBuilder();
    
            if (!line.hasOption("in") && !line.hasOption("source")) {
                sb.append("i or s ");
            }
    
            if (!line.hasOption("migrate")) {
                sb.append("m ");
            }
    
            if (sb.length() > 0) {
                writeLine("Missing required options: " + sb.toString());
                return;
            }
    
            File input = line.getOption("in");
            String migrate = line.getOption("migrate");
            String source = line.getOption("source");
            
            List<Long> db = line.getOptions("db");
            List<String> type = line.getOptions("type");
            boolean replace = line.hasOption("replace");
            List<String> regexs = line.getOptions("key");
    
            if (source == null && input != null) {
                URI u = input.toURI();
                source = new URI("redis", u.getRawAuthority(), u.getRawPath(), u.getRawQuery(), u.getRawFragment()).toString();
            }
    
            RedisURI uri = new RedisURI(migrate);
            if (uri.getFileType() != null) {
                writeLine("Invalid uri: " + migrate);
                return;
            }
            Replicator r = new RedisReplicator(source);
            r.setRdbVisitor(new MigrateRdbVisitor(r, migrate, db, regexs, Type.parse(type), replace));
            r.open();
        }
    }
    
    @Override
    public String name() {
        return "rmt";
    }
    
    public static void run(String[] args) throws Exception {
        RmtCommand command = new RmtCommand();
        command.execute(args);
    }
}
