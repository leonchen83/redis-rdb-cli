package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.CliRedisReplicator;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Escape;
import com.moilioncircle.redis.cli.tool.glossary.Format;
import com.moilioncircle.redis.cli.tool.util.ProgressBar;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;
import java.util.List;

import static com.moilioncircle.redis.cli.tool.glossary.Phase.RDB;

/**
 * @author Baoyi Chen
 */
public class RctCommand extends AbstractCommand {

    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rct usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rct version.").build();
    private static final Option FORMAT = Option.builder("f").longOpt("format").required(false).hasArg().argName("format").type(String.class).desc("format to export. valid commands are json, diff, dump, key, keyval, mem and resp").build();
    private static final Option SOURCE = Option.builder("s").longOpt("source").required(false).hasArg().argName("source").type(String.class).desc("<source> eg:\n /path/to/dump.rdb redis://host:port?authPassword=foobar redis:///path/to/dump.rdb.").build();
    private static final Option OUTPUT = Option.builder("o").longOpt("out").required(false).hasArg().argName("file").type(File.class).desc("output file.").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("database number. multiple databases can be provided. if not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex regex...").valueSeparator(' ').type(String.class).desc("keys to export. this can be a regex. if not specified, all keys will be returned.").build();
    private static final Option TYPE = Option.builder("t").longOpt("type").required(false).hasArgs().argName("type type...").valueSeparator(' ').type(String.class).desc("data type to export. possible values are string, hash, set, sortedset, list, module, stream. multiple types can be provided. if not specified, all data types will be returned.").build();
    private static final Option BYTES = Option.builder("b").longOpt("bytes").required(false).hasArgs().argName("bytes").type(Number.class).desc("limit memory output(--format mem) to keys greater to or equal to this value (in bytes)").build();
    private static final Option LARGEST = Option.builder("l").longOpt("largest").required(false).hasArg().argName("n").type(Number.class).desc("limit memory output(--format mem) to only the top n keys (by size).").build();
    private static final Option ESCAPE = Option.builder("e").longOpt("escape").required(false).hasArg().argName("escape").type(String.class).desc("escape strings to encoding: redis (default), raw.").build();

    private static final String HEADER = "rct -f <format> -s <source> -o <file> [-d <num num...>] [-e <escape>] [-k <regex regex...>] [-t <type type...>] [-b <bytes>] [-l <n>]";
    private static final String EXAMPLE = "examples:\n rct -f resp -s redis://127.0.0.1:6379 -o ./target.aof -d 0 1\n rct -f json -s ./dump.rdb -o ./target.json -k user.* product.*\n rct -f mem -s ./dump.rdb -o ./target.aof -e redis -t list -l 10 -b 1024\n";

    @Override
    public String name() {
        return "rct";
    }

    public RctCommand() {
        addOption(HELP);
        addOption(VERSION);
        addOption(FORMAT);
        addOption(SOURCE);
        addOption(OUTPUT);
        addOption(DB);
        addOption(KEY);
        addOption(TYPE);
        addOption(BYTES);
        addOption(LARGEST);
        addOption(ESCAPE);
    }

    @Override
    protected void doExecute(CommandLine line) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(HEADER, "options:", options, EXAMPLE);
        } else if (line.hasOption("version")) {
            writeLine(version());
        } else {
            StringBuilder sb = new StringBuilder();
            if (!line.hasOption("format")) {
                sb.append("f ");
            }

            if (!line.hasOption("source")) {
                sb.append("s ");
            }

            if (!line.hasOption("out")) {
                sb.append("o ");
            }

            if (sb.length() > 0) {
                writeLine("Missing required options: " + sb.toString() + ", Try `rct -h` for more information.");
                return;
            }

            String source = line.getOption("source");
            File output = line.getOption("out");
            String format = line.getOption("format");

            List<Long> db = line.getOptions("db");
            Long bytes = line.getOption("bytes");
            Long largest = line.getOption("largest");
            String escape = line.getOption("escape");
            List<String> type = line.getOptions("type");
            List<String> regexs = line.getOptions("key");

            source = normalize(source, FileType.RDB, "Invalid options: s, Try `rct -h` for more information.");

            try (ProgressBar bar = new ProgressBar(-1)) {
                Configure configure = Configure.bind();
                Replicator r = new CliRedisReplicator(source, configure);
                r.addExceptionListener((rep, tx, e) -> { throw new RuntimeException(tx.getMessage(), tx); });
                Format.parse(format).dress(r, configure, output, db, regexs, largest, bytes, DataType.parse(type), Escape.parse(escape));
                Runtime.getRuntime().addShutdownHook(new Thread(() -> CliRedisReplicator.closeQuietly(r)));
                r.addEventListener((rep, event) -> {
                    if (event instanceof PreRdbSyncEvent)
                        rep.addRawByteListener(b -> bar.react(b.length, RDB));
                    if (event instanceof PostRdbSyncEvent) CliRedisReplicator.closeQuietly(rep);
                });
                r.open();
            }
        }
    }

    public static void run(String[] args) throws Exception {
        RctCommand command = new RctCommand();
        command.execute(args);
    }
}
