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

package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.CliRedisReplicator;
import com.moilioncircle.redis.cli.tool.ext.rmt.ClusterRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rmt.MigrateRdbVisitor;
import com.moilioncircle.redis.cli.tool.util.ProgressBar;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static com.moilioncircle.redis.cli.tool.glossary.DataType.parse;

/**
 * @author Baoyi Chen
 */
public class RmtCommand extends AbstractCommand {

    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rmt usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rmt version.").build();
    private static final Option SOURCE = Option.builder("s").longOpt("source").required(false).hasArg().argName("source").type(String.class).desc("<source> eg:\n /path/to/dump.rdb redis://host:port?authPassword=foobar redis:///path/to/dump.rdb").build();
    private static final Option REPLACE = Option.builder("r").longOpt("replace").required(false).desc("replace exist key value. if not specified, default value is false.").build();
    private static final Option CONFIG = Option.builder("c").longOpt("config").required(false).hasArg().argName("file").type(File.class).desc("migrate data to cluster via redis cluster's <nodes.conf> file, if specified, no need to specify --migrate.").build();
    private static final Option MIGRATE = Option.builder("m").longOpt("migrate").required(false).hasArg().argName("uri").type(String.class).desc("migrate to uri. eg: redis://host:port?authPassword=foobar.").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("database number. multiple databases can be provided. if not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex regex...").valueSeparator(' ').type(String.class).desc("keys to export. this can be a regex. if not specified, all keys will be returned.").build();
    private static final Option TYPE = Option.builder("t").longOpt("type").required(false).hasArgs().argName("type type...").valueSeparator(' ').type(String.class).desc("data type to export. possible values are string, hash, set, sortedset, list, module, stream. multiple types can be provided. if not specified, all data types will be returned.").build();

    private static final String HEADER = "rmt -s <source> [-m <uri> | -c <file>] [-d <num num...>] [-k <regex regex...>] [-t <type type...>] [-r]";
    private static final String EXAMPLE = "\nexamples:\n rmt -s ./dump.rdb -c ./nodes.conf -t string -r\n rmt -s ./dump.rdb -m redis://127.0.0.1:6380 -t list -d 0\n rmt -s redis://120.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0\n";

    private RmtCommand() {
        addOption(HELP);
        addOption(VERSION);
        addOption(SOURCE);
        addOption(REPLACE);
        addOption(CONFIG);
        addOption(MIGRATE);
        addOption(DB);
        addOption(KEY);
        addOption(TYPE);
    }

    @Override
    protected void doExecute(CommandLine line) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(HEADER, "\noptions:", options, EXAMPLE);
        } else if (line.hasOption("version")) {
            writeLine(version());
        } else {
            StringBuilder sb = new StringBuilder();

            if (!line.hasOption("source")) {
                sb.append("s ");
            }

            if (line.hasOption("migrate") && line.hasOption("config")) {
                sb.append("m or c ");
            } else if (!line.hasOption("migrate") && !line.hasOption("config")) {
                sb.append("m or c ");
            }

            if (sb.length() > 0) {
                writeLine("Missing required options: " + sb.toString() + ". Try `rmt -h` for more information.");
                return;
            }

            File conf = line.getOption("config");
            String source = line.getOption("source");
            String migrate = line.getOption("migrate");

            List<Long> db = line.getOptions("db");
            List<String> type = line.getOptions("type");
            boolean replace = line.hasOption("replace");
            List<String> regexs = line.getOptions("key");

            source = normalize(source, FileType.RDB, "Invalid options: s. Try `rmt -h` for more information.");

            Configure configure = Configure.bind();

            if (migrate != null) {
                RedisURI uri = new RedisURI(migrate);
                if (uri.getFileType() != null) {
                    writeLine("Invalid options: m. Try `rmt -h` for more information.");
                    return;
                }
                try (ProgressBar bar = new ProgressBar(-1)) {
                    Replicator r = new CliRedisReplicator(source, configure);
                    r.setRdbVisitor(new MigrateRdbVisitor(r, configure, migrate, db, regexs, parse(type), replace));
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> CliRedisReplicator.closeQuietly(r)));
                    r.addExceptionListener((rep, tx, e) -> { throw new RuntimeException(tx.getMessage(), tx); });
                    r.addEventListener((rep, event) -> {
                        if (event instanceof PreRdbSyncEvent)
                            rep.addRawByteListener(b -> bar.react(b.length));
                        if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent)
                            CliRedisReplicator.closeQuietly(rep);
                    });
                    r.open();
                }
            } else {
                if (conf == null || !Files.exists(conf.toPath())) {
                    writeLine("Invalid options: c. Try `rmt -h` for more information.");
                    return;
                }
                try (ProgressBar bar = new ProgressBar(-1)) {
                    Replicator r = new CliRedisReplicator(source, configure);
                    r.setRdbVisitor(new ClusterRdbVisitor(r, configure, conf, db, regexs, parse(type), replace));
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> CliRedisReplicator.closeQuietly(r)));
                    r.addExceptionListener((rep, tx, e) -> { throw new RuntimeException(tx.getMessage(), tx); });
                    r.addEventListener((rep, event) -> {
                        if (event instanceof PreRdbSyncEvent)
                            rep.addRawByteListener(b -> bar.react(b.length));
                        if (event instanceof PostRdbSyncEvent || event instanceof PreCommandSyncEvent)
                            CliRedisReplicator.closeQuietly(rep);
                    });
                    r.open();
                }
            }
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
