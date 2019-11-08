/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.rdb.cli.cmd;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.glossary.Action;
import com.moilioncircle.redis.rdb.cli.glossary.DataType;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class RdtCommand extends AbstractCommand {

    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rdt usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rdt version.").build();
    private static final Option SPLIT = Option.builder("s").longOpt("split").required(false).hasArg().argName("source").type(String.class).desc("split rdb to multi rdb files via cluster's <nodes.conf>. eg:\n /path/to/dump.rdb redis://host:port?authPassword=foobar redis:///path/to/dump").build();
    private static final Option MERGE = Option.builder("m").longOpt("merge").required(false).hasArgs().argName("file file...").valueSeparator(' ').type(File.class).desc("merge multi rdb files to one rdb file.").build();
    private static final Option BACKUP = Option.builder("b").longOpt("backup").required(false).hasArg().argName("source").type(String.class).desc("backup <source> to local rdb file. eg: /path/to/dump.rdb redis://host:port?authPassword=foobar redis:///path/to/dump.rdb").build();
    private static final Option OUTPUT = Option.builder("o").longOpt("out").required(false).hasArg().argName("file").type(String.class).desc("if --backup <source> or --merge <file file...> specified. the <file> is the target file. if --split <source> specified. the <file> is the target path.").build();
    private static final Option CONFIG = Option.builder("c").longOpt("config").required(false).hasArg().argName("file").type(File.class).desc("redis cluster's <nodes.conf> file(--split <source>).").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("database number. multiple databases can be provided. if not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex regex...").valueSeparator(' ').type(String.class).desc("keys to export. this can be a regex. if not specified, all keys will be returned.").build();
    private static final Option TYPE = Option.builder("t").longOpt("type").required(false).hasArgs().argName("type type...").valueSeparator(' ').type(String.class).desc("data type to export. possible values are string, hash, set, sortedset, list, module, stream. multiple types can be provided. if not specified, all data types will be returned.").build();

    private static final String HEADER = "rdt [-b <source> | -s <source> -c <file> | -m <file file...>] -o <file> [-d <num num...>] [-k <regex regex...>] [-t <type type...>]";
    private static final String EXAMPLE = "\nexamples:\n rdt -b ./dump.rdb -o ./dump.rdb1 -d 0 1\n rdt -b redis://127.0.0.1:6379 -o ./dump.rdb -k user.*\n rdt -m ./dump1.rdb ./dump2.rdb -o ./dump.rdb -t hash\n rdt -s ./dump.rdb -c ./nodes.conf -o /path/to/folder -t hash -d 0\n rdt -s redis://127.0.0.1:6379 -c ./nodes.conf -o /path/to/folder -d 0\n";

    private RdtCommand() {
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
    protected void doExecute(CommandLine line, Configure configure) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(HEADER, "\noptions:", options, EXAMPLE);
        } else if (line.hasOption("version")) {
            writeLine(version());
        } else {
            String split = line.getOption("split");
            String backup = line.getOption("backup");
            List<File> merge = line.getOptions("merge");

            List<Long> db = line.getOptions("db");
            List<String> type = line.getOptions("type");
            List<String> regexs = line.getOptions("key");

            File conf = line.getOption("config");
            String output = line.getOption("out");

            if (output == null) {
                writeError("Missing required options: o. Try `rdt -h` for more information.");
                return;
            }

            if (split != null && backup != null && !merge.isEmpty()) {
                writeError("Invalid options: s or b or m. Try `rdt -h` for more information.");
                return;
            }

            if ((split != null && backup != null) || (backup != null && !merge.isEmpty()) || (split != null && !merge.isEmpty())) {
                writeError("Invalid options: s or b or m. Try `rdt -h` for more information.");
                return;
            }

            Action action = Action.NONE;
            if (split != null) {
                split = normalize(split, FileType.RDB, "Invalid options: s. Try `rdt -h` for more information.");
                Path path = Paths.get(output);
                if (Files.exists(path) && !Files.isDirectory(Paths.get(output))) {
                    writeError("Invalid options: o. Try `rdt -h` for more information.");
                    return;
                }
                if (conf == null) {
                    writeError("Missing required options: c. Try `rdt -h` for more information.");
                    return;
                }
                action = Action.SPLIT;
            } else if (backup != null) {
                backup = normalize(backup, FileType.RDB, "Invalid options: b. Try `rdt -h` for more information.");
                Path path = Paths.get(output);
                if (Files.exists(path) && !Files.isRegularFile(path)) {
                    writeError("Invalid options: o. Try `rdt -h` for more information.");
                    return;
                }
                action = Action.BACKUP;
            } else if (merge != null) {
                Path path = Paths.get(output);
                if (Files.exists(path) && !Files.isRegularFile(path)) {
                    writeError("Invalid options: o. Try `rdt -h` for more information.");
                    return;
                }
                action = Action.MERGE;
            }

            try (ProgressBar bar = new ProgressBar(-1)) {
                List<Tuple2<Replicator, String>> list = action.dress(configure, split, backup, merge, output, db, regexs, conf, DataType.parse(type));
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    for (Tuple2<Replicator, String> tuple : list) Replicators.closeQuietly(tuple.getV1());
                }));

                for (Tuple2<Replicator, String> tuple : list) {
                    tuple.getV1().addExceptionListener((rep, tx, e) -> {
                        throw new RuntimeException(tx.getMessage(), tx);
                    });
                    tuple.getV1().addEventListener((rep, event) -> {
                        if (event instanceof PreRdbSyncEvent)
                            rep.addRawByteListener(b -> bar.react(b.length, tuple.getV2()));
                        if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent)
                            Replicators.closeQuietly(rep);
                    });
                    tuple.getV1().open();
                }
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
