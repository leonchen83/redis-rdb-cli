package com.moilioncircle.redis.cli.tool.cmd;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;

/**
 * @author Baoyi Chen
 */
public class RdtCommand extends AbstractCommand {

    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rdt usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rdt version.").build();
    private static final Option SPLIT = Option.builder("s").longOpt("split").required(false).hasArg().argName("uri").type(String.class).desc("split uri to multi file via cluster's <node.conf>. eg: redis://host:port?authPassword=foobar redis:///path/to/dump.rdb redis:///path/to/appendonly.aof.").build();
    private static final Option MERGE = Option.builder("m").longOpt("merge").required(false).hasArgs().argName("file file...").valueSeparator(' ').type(File.class).desc("merge multi file to one file. if file contains aof format. then will generate a mixed file that can read via redis-4.x+.").build();
    private static final Option BACKUP = Option.builder("b").longOpt("backup").required(false).hasArg().argName("uri").type(String.class).desc("backup uri to local rdb file. eg: redis://host:port?authPassword=foobar redis:///path/to/dump.rdb").build();
    private static final Option OUTPUT = Option.builder("o").longOpt("out").required(false).hasArg().argName("file").type(File.class).desc("output file(--backup <uri> or --merge <file file...>).").build();
    private static final Option CONFIG = Option.builder("c").longOpt("config").required(false).hasArg().argName("file").type(File.class).desc("redis cluster's <node.conf> file(--split <file>).").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("database number. multiple databases can be provided. if not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex regex...").valueSeparator(' ').type(String.class).desc("keys to export. this can be a regex. if not specified, all keys will be returned.").build();
    private static final Option TYPE = Option.builder("t").longOpt("type").required(false).hasArgs().argName("type type...").valueSeparator(' ').type(String.class).desc("data type to export. possible values are string, hash, set, sortedset, list, module, stream. multiple types can be provided. if not specified, all data types will be returned.").build();

    public RdtCommand() {
        addOption(HELP);
        addOption(VERSION);
        addOption(SPLIT);
        addOption(MERGE);
        addOption(BACKUP);
        addOption(OUTPUT);
        addOption(CONFIG);
        addOption(DB);
        addOption(KEY);
        addOption(TYPE);
    }

    @Override
    protected void doExecute(CommandLine line) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("rdt", options);
        } else if (line.hasOption("version")) {
            writeLine(version());
        } else {
            StringBuilder sb = new StringBuilder();

            if (!line.hasOption("in") && !line.hasOption("source")) {
                sb.append("[i or s] ");
            }

            if (!line.hasOption("migrate")) {
                sb.append("m ");
            }

            if (sb.length() > 0) {
                writeLine("Missing required options: " + sb.toString() + ". Try `rdt -h` for more information.");
                return;
            }
        }
    }

    @Override
    public String name() {
        return "rdt";
    }

    public static void run(String[] args) throws Exception {
        RdtCommand command = new RdtCommand();
        command.execute(args);
    }
}
