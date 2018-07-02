package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Format;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PostFullSyncEvent;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class RctCommand extends AbstractCommand {

    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rct usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rct version.").build();
    private static final Option FORMAT = Option.builder("f").longOpt("format").required(false).hasArg().argName("format").type(String.class).desc("Command to execute. Valid commands are json, dump, key, val, mem and resp").build();
    private static final Option INPUT = Option.builder("i").longOpt("in").required(false).hasArg().argName("file").type(File.class).desc("Input file").build();
    private static final Option OUTPUT = Option.builder("o").longOpt("out").required(false).hasArg().argName("file").type(File.class).desc("Output file").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("db num").type(Number.class).desc("Database Number. Multiple databases can be provided. If not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex").type(String.class).desc("Keys to export. This can be a RegEx").build();
    private static final Option TYPE = Option.builder("r").longOpt("redis-type").required(false).hasArgs().argName("redis type").valueSeparator(',').desc("Data type to include. Possible values are string, hash, set, sortedset, list, module, stream. Multiple types can be provided. If not specified, all data types will be returned").build();
    private static final Option TOP = Option.builder("t").longOpt("top").required(false).hasArg().argName("n").type(Number.class).desc("Limit memory output to only the top N keys (by size)").build();
    private static final Option ESCAPE = Option.builder("e").longOpt("escape").required(false).hasArg().argName("escape").type(String.class).desc("Escape strings to encoding: raw bytes (default), print, utf8, or base64.").build();

    @Override
    public String name() {
        return "rct";
    }

    public RctCommand() {
        addOption(HELP);
        addOption(VERSION);
        addOption(FORMAT);
        addOption(INPUT);
        addOption(OUTPUT);
        addOption(DB);
        addOption(KEY);
        addOption(TYPE);
        addOption(TOP);
        addOption(ESCAPE);
    }

    @Override
    protected void doExecute(CommandLine line) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("rct", options);
        } else if (line.hasOption("version")) {
            writeLine(version());
        } else {
            StringBuilder sb = new StringBuilder();
            if (!line.hasOption("format")) {
                sb.append("f ");
            }

            if (!line.hasOption("in")) {
                sb.append("i ");
            }

            if (!line.hasOption("out")) {
                sb.append("o ");
            }

            if (sb.length() > 0) {
                writeLine("Missing required options: " + sb.toString());
                return;
            }

            String format = line.getOption("format");
            String input = line.getOption("in");
            File output = line.getOption("out");

            Long db = line.getOption("db");
            String keyRegEx = line.getOption("key");
            List<String> type = line.getOptions("redis-type");
            Long top = line.getOption("top");
            String escape = line.getOption("escape");

            Replicator r = new RedisReplicator(input);
            Format.parse(format).dress(r, output, db, keyRegEx, top, Type.parse(type), Escape.parse(escape));
            r.addEventListener((replicator, event) -> {
                if (event instanceof PostFullSyncEvent) {
                    try {
                        replicator.close();
                    } catch (IOException e) {
                    }
                }
            });
            r.open();
        }
    }

    public static void run(String[] args) throws Exception {
        RctCommand command = new RctCommand();
        command.execute(args);
    }
}
