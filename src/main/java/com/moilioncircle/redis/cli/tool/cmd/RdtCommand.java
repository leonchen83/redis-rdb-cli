package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.CliRedisReplicator;
import com.moilioncircle.redis.cli.tool.glossary.DataType;
import com.moilioncircle.redis.cli.tool.glossary.Type;
import com.moilioncircle.redis.cli.tool.util.ProgressBar;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PostFullSyncEvent;
import com.moilioncircle.redis.replicator.event.PreFullSyncEvent;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

import static com.moilioncircle.redis.cli.tool.util.ProgressBar.Phase.RDB;

/**
 * @author Baoyi Chen
 */
public class RdtCommand extends AbstractCommand {
    
    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rdt usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rdt version.").build();
    private static final Option SPLIT = Option.builder("s").longOpt("split").required(false).hasArg().argName("uri").type(String.class).desc("split uri to multi file via cluster's <node.conf>. eg: redis://host:port?authPassword=foobar redis:///path/to/dump").build();
    private static final Option MERGE = Option.builder("m").longOpt("merge").required(false).hasArgs().argName("file file...").valueSeparator(' ').type(File.class).desc("merge multi rdb files to one rdb file.").build();
    private static final Option BACKUP = Option.builder("b").longOpt("backup").required(false).hasArg().argName("uri").type(String.class).desc("backup uri to local rdb file. eg: redis://host:port?authPassword=foobar redis:///path/to/dump.rdb").build();
    private static final Option OUTPUT = Option.builder("o").longOpt("out").required(false).hasArg().argName("file").type(String.class).desc("if --backup <uri> or --merge <file file...> specified. the <file> is the target file. if --split <uri> specified. the <file> is the target path.").build();
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
            String split = line.getOption("split");
            String backup = line.getOption("backup");
            List<File> merge = line.getOptions("merge");
            
            List<Long> db = line.getOptions("db");
            List<String> type = line.getOptions("type");
            List<String> regexs = line.getOptions("key");
    
            String conf = line.getOption("config");
            String output = line.getOption("out");
    
            if (split == null && backup == null && merge.isEmpty()) {
                writeLine("Missing required options: s or b or m, `rdt -h` for more information.");
                return;
            }
    
            if (output == null) {
                writeLine("Missing required options: o, `rdt -h` for more information.");
                return;
            }
            
            if (split != null && backup != null && !merge.isEmpty()) {
                writeLine("Invalid options: s or b or m, `rdt -h` for more information.");
                return;
            }
    
            if ((split != null && backup != null) || (backup != null && !merge.isEmpty()) || (split != null && !merge.isEmpty())) {
                writeLine("Invalid options: s or b or m, `rdt -h` for more information.");
                return;
            }
            
            Configure configure = Configure.bind();
            Type rdtType = Type.NONE;
            if (split != null) {
                if (!Paths.get(output).toFile().isDirectory()) {
                    writeLine("Invalid options: o, `rdt -h` for more information.");
                    return;
                }
                if (conf == null) {
                    writeLine("Missing required options: c, `rdt -h` for more information.");
                    return;
                }
                if (!Paths.get(conf).toFile().isFile()) {
                    writeLine("Invalid options: c, `rdt -h` for more information.");
                    return;
                }
                rdtType = Type.SPLIT;
            } else if (backup != null) {
                if (!Paths.get(output).toFile().isFile()) {
                    writeLine("Invalid options: o, `rdt -h` for more information.");
                    return;
                }
                rdtType = Type.BACKUP;
            } else if (merge != null) {
                if (!Paths.get(output).toFile().isFile()) {
                    writeLine("Invalid options: o, `rdt -h` for more information.");
                    return;
                }
                rdtType = Type.MERGE;
            }
    
            ProgressBar bar = new ProgressBar(-1);
    
            List<Replicator> list = rdtType.dress(configure, split, backup, merge, output, db, regexs, conf, DataType.parse(type));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                for (Replicator r : list) CliRedisReplicator.closeQuietly(r);
            }));
    
            for (Replicator r : list) {
                r.addEventListener((rep, event) -> {
                    if (event instanceof PreFullSyncEvent)
                        rep.addRawByteListener(b -> bar.react(b.length, RDB));
                    if (event instanceof PostFullSyncEvent) CliRedisReplicator.close(rep);
                });
                r.open();
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
