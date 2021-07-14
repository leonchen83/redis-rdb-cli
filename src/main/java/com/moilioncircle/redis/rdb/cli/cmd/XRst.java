/*
 * Copyright 2016-2017 Leon Chen
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

import static com.moilioncircle.redis.rdb.cli.util.XUris.normalize;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import com.moilioncircle.redis.rdb.cli.api.sink.cmd.CombineCommandParser;
import com.moilioncircle.redis.rdb.cli.cmd.support.XVersionProvider;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.CliRedisReplicator;
import com.moilioncircle.redis.rdb.cli.ext.rst.ClusterRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rst.SingleRdbVisitor;
import com.moilioncircle.redis.rdb.cli.net.impl.XEndpoint;
import com.moilioncircle.redis.rdb.cli.net.protocol.RedisObject;
import com.moilioncircle.redis.rdb.cli.util.ProgressBar;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.parser.AppendParser;
import com.moilioncircle.redis.replicator.cmd.parser.BLMoveParser;
import com.moilioncircle.redis.replicator.cmd.parser.BRPopLPushParser;
import com.moilioncircle.redis.replicator.cmd.parser.BitFieldParser;
import com.moilioncircle.redis.replicator.cmd.parser.BitOpParser;
import com.moilioncircle.redis.replicator.cmd.parser.CopyParser;
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
import com.moilioncircle.redis.replicator.cmd.parser.GeoSearchStoreParser;
import com.moilioncircle.redis.replicator.cmd.parser.GetSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.HDelParser;
import com.moilioncircle.redis.replicator.cmd.parser.HIncrByParser;
import com.moilioncircle.redis.replicator.cmd.parser.HMSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.HSetNxParser;
import com.moilioncircle.redis.replicator.cmd.parser.HSetParser;
import com.moilioncircle.redis.replicator.cmd.parser.IncrByParser;
import com.moilioncircle.redis.replicator.cmd.parser.IncrParser;
import com.moilioncircle.redis.replicator.cmd.parser.LInsertParser;
import com.moilioncircle.redis.replicator.cmd.parser.LMoveParser;
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
import com.moilioncircle.redis.replicator.cmd.parser.ZDiffStoreParser;
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

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * @author Baoyi Chen
 */
@Command(name = "rst",
		separator = " ",
		usageHelpWidth = 80,
		synopsisHeading = "",
		mixinStandardHelpOptions = true,
		optionListHeading = "%nOptions:%n",
		versionProvider = XVersionProvider.class,
		customSynopsis = {
				"Usage: rst [-hV] -s <uri> (-m <uri> | -c <config>) [-d <db>...] [-r] [-l]"
		},
		footer = {"%nExamples:",
				"  rst -s redis://127.0.0.1:6379 -c ./nodes.conf -r",
				"  rst -s redis://127.0.0.1:6379 -m redis://127.0.0.1:6380 -d 0"})
public class XRst implements Callable<Integer> {
	
	@Spec
	private CommandSpec spec;
	
	@ArgGroup(exclusive = true, multiplicity = "1")
	public RstExclusive exclusive;
	
	private static class RstExclusive {
		@Option(names = {"-m", "--migrate"}, required = true, paramLabel = "<uri>", description = {"Migrate to uri. eg:", "redis://host:port?authPassword=foobar."})
		private String migrate;
		
		@Option(names = {"-c", "--config"}, required = true, description = {"Migrate data to cluster via redis cluster's", "<nodes.conf> file, if specified, no need to", "specify --migrate."}, type = File.class)
		private File config;
	}
	
	@Option(names = {"-d", "--db"}, arity = "1..*", description = {"Database number. multiple databases can be", "provided. if not specified, all databases", "will be included."}, type = Long.class)
	private List<Long> db = new ArrayList<>();
	
	@Option(names = {"-s", "--source"}, required = true, paramLabel = "<uri>", description = {"Redis uri. eg:", "redis://host:port?authPassword=foobar"})
	private String source;
	
	@Option(names = {"-r", "--replace"}, description = {"Replace exist key value. if not specified,", "default value is false."})
	private boolean replace;
	
	@Option(names = {"-l", "--legacy"}, description = {"If specify the <replace> and this parameter.", "then use lua script to migrate data to target.", "if target redis version is greater than 3.0.", "no need to add this parameter."})
	private boolean legacy;
	
	
	@Override
	public Integer call() throws Exception {
		source = normalize(source, null, spec, "Invalid options: '--source=<uri>'");
		
		Configure configure = Configure.bind();
		if (exclusive.migrate != null) {
			RedisURI uri = new RedisURI(exclusive.migrate);
			if (uri.getFileType() != null) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--migrate=<uri>'");
			}
			try (ProgressBar bar = new ProgressBar(-1)) {
				Replicator r = new CliRedisReplicator(source, configure);
				r.setRdbVisitor(getRdbVisitor(r, configure, uri, db, replace, legacy));
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					Replicators.closeQuietly(r);
				}));
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
			if (exclusive.config == null || !Files.exists(exclusive.config.toPath())) {
				throw new ParameterException(spec.commandLine(), "Invalid options: '--config=<config>'");
			}
			try (ProgressBar bar = new ProgressBar(-1)) {
				Replicator r = new CliRedisReplicator(source, configure);
				List<String> lines = Files.readAllLines(exclusive.config.toPath());
				r.setRdbVisitor(new ClusterRdbVisitor(r, configure, lines, replace));
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					Replicators.closeQuietly(r);
				}));
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
		return 0;
	}
	
	private RdbVisitor getRdbVisitor(Replicator replicator, Configure configure, RedisURI uri, List<Long> db, boolean replace, boolean legacy) throws Exception {
		try (XEndpoint endpoint = new XEndpoint(uri.getHost(), uri.getPort(), configure.merge(uri, false), configure)) {
			RedisObject r = endpoint.send("cluster".getBytes(), "nodes".getBytes());
			if (r.type.isError()) {
				return new SingleRdbVisitor(replicator, configure, uri, db, replace, legacy);
			} else {
				String config = r.getString();
				List<String> lines = Arrays.asList(config.split("\n"));
				return new ClusterRdbVisitor(replicator, configure, lines, replace);
			}
		} catch (Throwable e) {
			throw new RuntimeException("failed to connect to " + uri.getHost() + ":" + uri.getPort() + ", reason " + e.getMessage());
		}
	}
	
	private Replicator dress(Replicator replicator) {
		replicator.addCommandParser(CommandName.name("SELECT"), new SelectParser());
		replicator.addCommandParser(CommandName.name("REPLCONF"), new ReplConfParser());
		//
		replicator.addCommandParser(CommandName.name("PING"), new CombineCommandParser(new PingParser()));
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
		// since redis 6.2
		replicator.addCommandParser(CommandName.name("COPY"), new CombineCommandParser(new CopyParser()));
		replicator.addCommandParser(CommandName.name("LMOVE"), new CombineCommandParser(new LMoveParser()));
		replicator.addCommandParser(CommandName.name("BLMOVE"), new CombineCommandParser(new BLMoveParser()));
		replicator.addCommandParser(CommandName.name("ZDIFFSTORE"), new CombineCommandParser(new ZDiffStoreParser()));
		replicator.addCommandParser(CommandName.name("GEOSEARCHSTORE"), new CombineCommandParser(new GeoSearchStoreParser()));
		return replicator;
	}
}
