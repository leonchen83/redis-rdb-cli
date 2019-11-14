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

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.CliRedisReplicator;
import com.moilioncircle.redis.rdb.cli.ext.rst.ClusterRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rst.SingleRdbVisitor;
import com.moilioncircle.redis.rdb.cli.net.Endpoint;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.parser.DefaultCommandParser;
import com.moilioncircle.redis.replicator.cmd.parser.PingParser;
import com.moilioncircle.redis.replicator.cmd.parser.SelectParser;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.RdbVisitor;

/**
 * @author Baoyi Chen
 */
public class RstCommand extends AbstractCommand {

    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rst usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rst version.").build();
    private static final Option SOURCE = Option.builder("s").longOpt("source").required(false).hasArg().argName("source").type(String.class).desc("<source> eg:\n redis://host:port?authPassword=foobar").build();
    private static final Option REPLACE = Option.builder("r").longOpt("replace").required(false).desc("replace exist key value. if not specified, default value is false.").build();
    private static final Option LEGACY = Option.builder("l").longOpt("legacy").required(false).desc("if specify the <replace> and this parameter. then use lua script to migrate data to target. if target redis version is greater than 3.0. no need to add this parameter.").build();
    private static final Option CONFIG = Option.builder("c").longOpt("config").required(false).hasArg().argName("file").type(File.class).desc("migrate data to cluster via redis cluster's <nodes.conf> file, if specified, no need to specify --migrate.").build();
    private static final Option MIGRATE = Option.builder("m").longOpt("migrate").required(false).hasArg().argName("uri").type(String.class).desc("migrate to uri. eg: redis://host:port?authPassword=foobar.").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("database number. multiple databases can be provided. if not specified, all databases will be included.").build();

    private static final String HEADER = "rst -s <source> [-m <uri> | -c <file>] [-d <num num...>] [-r] [-l]";
    private static final String EXAMPLE = "\nexamples:\n rst -s redis://127.0.0.1:6379 -c ./nodes.conf -r\n rst -s redis://120.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0\n";

    private RstCommand() {
        addOption(HELP);
        addOption(VERSION);
        addOption(SOURCE);
        addOption(REPLACE);
        addOption(CONFIG);
        addOption(MIGRATE);
        addOption(DB);
        addOption(LEGACY);
    }

    @Override
    @SuppressWarnings("all")
    protected void doExecute(CommandLine line, Configure configure) throws Exception {
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
                writeError("Missing required options: " + sb.toString() + ". Try `rst -h` for more information.");
                return;
            }

            File conf = line.getOption("config");
            String source = line.getOption("source");
            String migrate = line.getOption("migrate");

            List<Long> db = line.getOptions("db");
            boolean replace = line.hasOption("replace");
            boolean legacy = line.hasOption("legacy");

            source = normalize(source, null, "Invalid options: s. Try `rst -h` for more information.");

            if (migrate != null) {
                RedisURI uri = new RedisURI(migrate);
                if (uri.getFileType() != null) {
                    writeError("Invalid options: m. Try `rst -h` for more information.");
                    return;
                }
                try (ProgressBar bar = new ProgressBar(-1)) {
                    Replicator r = new CliRedisReplicator(source, configure);
                    r.setRdbVisitor(getRdbVisitor(r, configure, uri, db, replace, legacy));
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> Replicators.closeQuietly(r)));
                    r.addExceptionListener((rep, tx, e) -> {
                        throw new RuntimeException(tx.getMessage(), tx);
                    });
                    r.addEventListener((rep, event) -> {
                        if (event instanceof PreRdbSyncEvent)
                            rep.addRawByteListener(b -> bar.react(b.length));
                    });
                    dress(r).open();
                }
            } else {
                if (conf == null || !Files.exists(conf.toPath())) {
                    writeError("Invalid options: c. Try `rst -h` for more information.");
                    return;
                }
                try (ProgressBar bar = new ProgressBar(-1)) {
                    Replicator r = new CliRedisReplicator(source, configure);
                    List<String> lines = Files.readAllLines(conf.toPath());
                    r.setRdbVisitor(new ClusterRdbVisitor(r, configure, lines, replace));
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> Replicators.closeQuietly(r)));
                    r.addExceptionListener((rep, tx, e) -> {
                        throw new RuntimeException(tx.getMessage(), tx);
                    });
                    r.addEventListener((rep, event) -> {
                        if (event instanceof PreRdbSyncEvent)
                            rep.addRawByteListener(b -> bar.react(b.length));
                    });
                    dress(r).open();
                }
            }
        }
    }

    private RdbVisitor getRdbVisitor(Replicator replicator, Configure configure, RedisURI uri, List<Long> db, boolean replace, boolean legacy) throws Exception {
        try (Endpoint endpoint = new Endpoint(uri.getHost(), uri.getPort(), Configuration.valueOf(uri), configure)) {
            Endpoint.RedisObject r = endpoint.send("cluster".getBytes(), "nodes".getBytes());
            if (r.type.isError()) {
                return new SingleRdbVisitor(replicator, configure, uri, db, replace, legacy);
            } else {
                String config = r.getString();
                List<String> lines = Arrays.asList(config.split("\n"));
                return new ClusterRdbVisitor(replicator, configure, lines, replace);
            }
        }
    }

    private Replicator dress(Replicator replicator) {
        replicator.addCommandParser(CommandName.name("PING"), new PingParser());
        replicator.addCommandParser(CommandName.name("SELECT"), new SelectParser());
        replicator.addCommandParser(CommandName.name("APPEND"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SET"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SETEX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("MSET"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("DEL"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SADD"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("HMSET"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("HSET"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LSET"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("EXPIRE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("EXPIREAT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("GETSET"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("HSETNX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("MSETNX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PSETEX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SETNX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SETRANGE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("HDEL"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LPOP"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LPUSH"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LPUSHX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LRem"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RPOP"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RPUSH"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RPUSHX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZREM"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RENAME"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("INCR"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("DECR"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("INCRBY"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("DECRBY"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PERSIST"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("FLUSHALL"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("FLUSHDB"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("HINCRBY"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZINCRBY"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("MOVE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SMOVE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PFADD"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PFCOUNT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PFMERGE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SDIFFSTORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SINTERSTORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SUNIONSTORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZADD"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZINTERSTORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZUNIONSTORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("BRPOPLPUSH"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LINSERT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RENAMENX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RESTORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PEXPIRE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PEXPIREAT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("GEOADD"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("EVAL"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("EVALSHA"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SCRIPT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("PUBLISH"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("BITOP"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("BITFIELD"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SETBIT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SREM"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("UNLINK"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SWAPDB"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("MULTI"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("EXEC"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZREMRANGEBYSCORE"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZREMRANGEBYRANK"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZREMRANGEBYLEX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("LTRIM"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("SORT"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("RPOPLPUSH"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZPOPMIN"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("ZPOPMAX"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("REPLCONF"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XACK"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XADD"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XCLAIM"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XDEL"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XGROUP"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XTRIM"), new DefaultCommandParser());
        replicator.addCommandParser(CommandName.name("XSETID"), new DefaultCommandParser());
        return replicator;
    }

    @Override
    public String name() {
        return "rst";
    }

    public static void run(String[] args) throws Exception {
        RstCommand command = new RstCommand();
        command.execute(args);
    }
    

}
