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
import com.moilioncircle.redis.rdb.cli.ext.rst.cmd.CombineCommandParser;
import com.moilioncircle.redis.rdb.cli.monitor.MonitorManager;
import com.moilioncircle.redis.rdb.cli.net.Endpoint;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.parser.AppendParser;
import com.moilioncircle.redis.replicator.cmd.parser.BRPopLPushParser;
import com.moilioncircle.redis.replicator.cmd.parser.BitFieldParser;
import com.moilioncircle.redis.replicator.cmd.parser.BitOpParser;
import com.moilioncircle.redis.replicator.cmd.parser.DecrByParser;
import com.moilioncircle.redis.replicator.cmd.parser.DecrParser;
import com.moilioncircle.redis.replicator.cmd.parser.DelParser;
import com.moilioncircle.redis.replicator.cmd.parser.EvalParser;
import com.moilioncircle.redis.replicator.cmd.parser.EvalShaParser;
import com.moilioncircle.redis.replicator.cmd.parser.ExecParser;
import com.moilioncircle.redis.replicator.cmd.parser.ExpireAtParser;
import com.moilioncircle.redis.replicator.cmd.parser.ExpireParser;
import com.moilioncircle.redis.replicator.cmd.parser.FlushAllParser;
import com.moilioncircle.redis.replicator.cmd.parser.FlushDBParser;
import com.moilioncircle.redis.replicator.cmd.parser.GeoAddParser;
import com.moilioncircle.redis.replicator.cmd.parser.GetSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.HDelParser;
import com.moilioncircle.redis.replicator.cmd.parser.HIncrByParser;
import com.moilioncircle.redis.replicator.cmd.parser.HMSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.HSetNxParser;
import com.moilioncircle.redis.replicator.cmd.parser.HSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.IncrByParser;
import com.moilioncircle.redis.replicator.cmd.parser.IncrParser;
import com.moilioncircle.redis.replicator.cmd.parser.LInsertParser;
import com.moilioncircle.redis.replicator.cmd.parser.LPopParser;
import com.moilioncircle.redis.replicator.cmd.parser.LPushParser;
import com.moilioncircle.redis.replicator.cmd.parser.LPushXParser;
import com.moilioncircle.redis.replicator.cmd.parser.LRemParser;
import com.moilioncircle.redis.replicator.cmd.parser.LSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.LTrimParser;
import com.moilioncircle.redis.replicator.cmd.parser.MSetNxParser;
import com.moilioncircle.redis.replicator.cmd.parser.MSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.MoveParser;
import com.moilioncircle.redis.replicator.cmd.parser.MultiParser;
import com.moilioncircle.redis.replicator.cmd.parser.PExpireAtParser;
import com.moilioncircle.redis.replicator.cmd.parser.PExpireParser;
import com.moilioncircle.redis.replicator.cmd.parser.PFAddParser;
import com.moilioncircle.redis.replicator.cmd.parser.PFCountParser;
import com.moilioncircle.redis.replicator.cmd.parser.PFMergeParser;
import com.moilioncircle.redis.replicator.cmd.parser.PSetExParser;
import com.moilioncircle.redis.replicator.cmd.parser.PersistParser;
import com.moilioncircle.redis.replicator.cmd.parser.PingParser;
import com.moilioncircle.redis.replicator.cmd.parser.PublishParser;
import com.moilioncircle.redis.replicator.cmd.parser.RPopLPushParser;
import com.moilioncircle.redis.replicator.cmd.parser.RPopParser;
import com.moilioncircle.redis.replicator.cmd.parser.RPushParser;
import com.moilioncircle.redis.replicator.cmd.parser.RPushXParser;
import com.moilioncircle.redis.replicator.cmd.parser.RenameNxParser;
import com.moilioncircle.redis.replicator.cmd.parser.RenameParser;
import com.moilioncircle.redis.replicator.cmd.parser.ReplConfParser;
import com.moilioncircle.redis.replicator.cmd.parser.RestoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.SAddParser;
import com.moilioncircle.redis.replicator.cmd.parser.SDiffStoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.SInterStoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.SMoveParser;
import com.moilioncircle.redis.replicator.cmd.parser.SRemParser;
import com.moilioncircle.redis.replicator.cmd.parser.SUnionStoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.ScriptParser;
import com.moilioncircle.redis.replicator.cmd.parser.SelectParser;
import com.moilioncircle.redis.replicator.cmd.parser.SetBitParser;
import com.moilioncircle.redis.replicator.cmd.parser.SetExParser;
import com.moilioncircle.redis.replicator.cmd.parser.SetNxParser;
import com.moilioncircle.redis.replicator.cmd.parser.SetParser;
import com.moilioncircle.redis.replicator.cmd.parser.SetRangeParser;
import com.moilioncircle.redis.replicator.cmd.parser.SortParser;
import com.moilioncircle.redis.replicator.cmd.parser.SwapDBParser;
import com.moilioncircle.redis.replicator.cmd.parser.UnLinkParser;
import com.moilioncircle.redis.replicator.cmd.parser.XAckParser;
import com.moilioncircle.redis.replicator.cmd.parser.XAddParser;
import com.moilioncircle.redis.replicator.cmd.parser.XClaimParser;
import com.moilioncircle.redis.replicator.cmd.parser.XDelParser;
import com.moilioncircle.redis.replicator.cmd.parser.XGroupParser;
import com.moilioncircle.redis.replicator.cmd.parser.XSetIdParser;
import com.moilioncircle.redis.replicator.cmd.parser.XTrimParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZAddParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZIncrByParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZInterStoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZPopMaxParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZPopMinParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZRemParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZRemRangeByLexParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZRemRangeByRankParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZRemRangeByScoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.ZUnionStoreParser;
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

            Configure configure = Configure.bind();
            MonitorManager manager = new MonitorManager(configure);
            manager.open();
            try {
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
            } finally {
                MonitorManager.closeQuietly(manager);
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
        replicator.addCommandParser(CommandName.name("REPLCONF"), new ReplConfParser());
        //
        replicator.addCommandParser(CommandName.name("APPEND"), new CombineCommandParser(new AppendParser()));
        replicator.addCommandParser(CommandName.name("SET"), new CombineCommandParser(new SetParser()));
        replicator.addCommandParser(CommandName.name("SETEX"), new CombineCommandParser(new SetExParser()));
        replicator.addCommandParser(CommandName.name("MSET"), new CombineCommandParser(new MSetParser()));
        replicator.addCommandParser(CommandName.name("DEL"), new CombineCommandParser(new DelParser()));
        replicator.addCommandParser(CommandName.name("SADD"), new CombineCommandParser(new SAddParser()));
        replicator.addCommandParser(CommandName.name("HMSET"), new CombineCommandParser(new HMSetParser()));
        replicator.addCommandParser(CommandName.name("HSET"), new CombineCommandParser(new HSetParser()));
        replicator.addCommandParser(CommandName.name("LSET"), new CombineCommandParser(new LSetParser()));
        replicator.addCommandParser(CommandName.name("EXPIRE"), new CombineCommandParser(new ExpireParser()));
        replicator.addCommandParser(CommandName.name("EXPIREAT"), new CombineCommandParser(new ExpireAtParser()));
        replicator.addCommandParser(CommandName.name("GETSET"), new CombineCommandParser(new GetSetParser()));
        replicator.addCommandParser(CommandName.name("HSETNX"), new CombineCommandParser(new HSetNxParser()));
        replicator.addCommandParser(CommandName.name("MSETNX"), new CombineCommandParser(new MSetNxParser()));
        replicator.addCommandParser(CommandName.name("PSETEX"), new CombineCommandParser(new PSetExParser()));
        replicator.addCommandParser(CommandName.name("SETNX"), new CombineCommandParser(new SetNxParser()));
        replicator.addCommandParser(CommandName.name("SETRANGE"), new CombineCommandParser(new SetRangeParser()));
        replicator.addCommandParser(CommandName.name("HDEL"), new CombineCommandParser(new HDelParser()));
        replicator.addCommandParser(CommandName.name("LPOP"), new CombineCommandParser(new LPopParser()));
        replicator.addCommandParser(CommandName.name("LPUSH"), new CombineCommandParser(new LPushParser()));
        replicator.addCommandParser(CommandName.name("LPUSHX"), new CombineCommandParser(new LPushXParser()));
        replicator.addCommandParser(CommandName.name("LRem"), new CombineCommandParser(new LRemParser()));
        replicator.addCommandParser(CommandName.name("RPOP"), new CombineCommandParser(new RPopParser()));
        replicator.addCommandParser(CommandName.name("RPUSH"), new CombineCommandParser(new RPushParser()));
        replicator.addCommandParser(CommandName.name("RPUSHX"), new CombineCommandParser(new RPushXParser()));
        replicator.addCommandParser(CommandName.name("ZREM"), new CombineCommandParser(new ZRemParser()));
        replicator.addCommandParser(CommandName.name("RENAME"), new CombineCommandParser(new RenameParser()));
        replicator.addCommandParser(CommandName.name("INCR"), new CombineCommandParser(new IncrParser()));
        replicator.addCommandParser(CommandName.name("DECR"), new CombineCommandParser(new DecrParser()));
        replicator.addCommandParser(CommandName.name("INCRBY"), new CombineCommandParser(new IncrByParser()));
        replicator.addCommandParser(CommandName.name("DECRBY"), new CombineCommandParser(new DecrByParser()));
        replicator.addCommandParser(CommandName.name("PERSIST"), new CombineCommandParser(new PersistParser()));
        replicator.addCommandParser(CommandName.name("FLUSHALL"), new CombineCommandParser(new FlushAllParser()));
        replicator.addCommandParser(CommandName.name("FLUSHDB"), new CombineCommandParser(new FlushDBParser()));
        replicator.addCommandParser(CommandName.name("HINCRBY"), new CombineCommandParser(new HIncrByParser()));
        replicator.addCommandParser(CommandName.name("ZINCRBY"), new CombineCommandParser(new ZIncrByParser()));
        replicator.addCommandParser(CommandName.name("MOVE"), new CombineCommandParser(new MoveParser()));
        replicator.addCommandParser(CommandName.name("SMOVE"), new CombineCommandParser(new SMoveParser()));
        replicator.addCommandParser(CommandName.name("PFADD"), new CombineCommandParser(new PFAddParser()));
        replicator.addCommandParser(CommandName.name("PFCOUNT"), new CombineCommandParser(new PFCountParser()));
        replicator.addCommandParser(CommandName.name("PFMERGE"), new CombineCommandParser(new PFMergeParser()));
        replicator.addCommandParser(CommandName.name("SDIFFSTORE"), new CombineCommandParser(new SDiffStoreParser()));
        replicator.addCommandParser(CommandName.name("SINTERSTORE"), new CombineCommandParser(new SInterStoreParser()));
        replicator.addCommandParser(CommandName.name("SUNIONSTORE"), new CombineCommandParser(new SUnionStoreParser()));
        replicator.addCommandParser(CommandName.name("ZADD"), new CombineCommandParser(new ZAddParser()));
        replicator.addCommandParser(CommandName.name("ZINTERSTORE"), new CombineCommandParser(new ZInterStoreParser()));
        replicator.addCommandParser(CommandName.name("ZUNIONSTORE"), new CombineCommandParser(new ZUnionStoreParser()));
        replicator.addCommandParser(CommandName.name("BRPOPLPUSH"), new CombineCommandParser(new BRPopLPushParser()));
        replicator.addCommandParser(CommandName.name("LINSERT"), new CombineCommandParser(new LInsertParser()));
        replicator.addCommandParser(CommandName.name("RENAMENX"), new CombineCommandParser(new RenameNxParser()));
        replicator.addCommandParser(CommandName.name("RESTORE"), new CombineCommandParser(new RestoreParser()));
        replicator.addCommandParser(CommandName.name("PEXPIRE"), new CombineCommandParser(new PExpireParser()));
        replicator.addCommandParser(CommandName.name("PEXPIREAT"), new CombineCommandParser(new PExpireAtParser()));
        replicator.addCommandParser(CommandName.name("GEOADD"), new CombineCommandParser(new GeoAddParser()));
        replicator.addCommandParser(CommandName.name("EVAL"), new CombineCommandParser(new EvalParser()));
        replicator.addCommandParser(CommandName.name("EVALSHA"), new CombineCommandParser(new EvalShaParser()));
        replicator.addCommandParser(CommandName.name("SCRIPT"), new CombineCommandParser(new ScriptParser()));
        replicator.addCommandParser(CommandName.name("PUBLISH"), new CombineCommandParser(new PublishParser()));
        replicator.addCommandParser(CommandName.name("BITOP"), new CombineCommandParser(new BitOpParser()));
        replicator.addCommandParser(CommandName.name("BITFIELD"), new CombineCommandParser(new BitFieldParser()));
        replicator.addCommandParser(CommandName.name("SETBIT"), new CombineCommandParser(new SetBitParser()));
        replicator.addCommandParser(CommandName.name("SREM"), new CombineCommandParser(new SRemParser()));
        replicator.addCommandParser(CommandName.name("UNLINK"), new CombineCommandParser(new UnLinkParser()));
        replicator.addCommandParser(CommandName.name("SWAPDB"), new CombineCommandParser(new SwapDBParser()));
        replicator.addCommandParser(CommandName.name("MULTI"), new CombineCommandParser(new MultiParser()));
        replicator.addCommandParser(CommandName.name("EXEC"), new CombineCommandParser(new ExecParser()));
        replicator.addCommandParser(CommandName.name("ZREMRANGEBYSCORE"), new CombineCommandParser(new ZRemRangeByScoreParser()));
        replicator.addCommandParser(CommandName.name("ZREMRANGEBYRANK"), new CombineCommandParser(new ZRemRangeByRankParser()));
        replicator.addCommandParser(CommandName.name("ZREMRANGEBYLEX"), new CombineCommandParser(new ZRemRangeByLexParser()));
        replicator.addCommandParser(CommandName.name("LTRIM"), new CombineCommandParser(new LTrimParser()));
        replicator.addCommandParser(CommandName.name("SORT"), new CombineCommandParser(new SortParser()));
        replicator.addCommandParser(CommandName.name("RPOPLPUSH"), new CombineCommandParser(new RPopLPushParser()));
        replicator.addCommandParser(CommandName.name("ZPOPMIN"), new CombineCommandParser(new ZPopMinParser()));
        replicator.addCommandParser(CommandName.name("ZPOPMAX"), new CombineCommandParser(new ZPopMaxParser()));
        replicator.addCommandParser(CommandName.name("XACK"), new CombineCommandParser(new XAckParser()));
        replicator.addCommandParser(CommandName.name("XADD"), new CombineCommandParser(new XAddParser()));
        replicator.addCommandParser(CommandName.name("XCLAIM"), new CombineCommandParser(new XClaimParser()));
        replicator.addCommandParser(CommandName.name("XDEL"), new CombineCommandParser(new XDelParser()));
        replicator.addCommandParser(CommandName.name("XGROUP"), new CombineCommandParser(new XGroupParser()));
        replicator.addCommandParser(CommandName.name("XTRIM"), new CombineCommandParser(new XTrimParser()));
        replicator.addCommandParser(CommandName.name("XSETID"), new CombineCommandParser(new XSetIdParser()));
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
