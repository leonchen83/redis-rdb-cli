package com.moilioncircle.redis.cli.tool.cmd;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.cli.tool.ext.MigrateRdbVisitor;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.parser.DefaultCommandParser;
import com.moilioncircle.redis.replicator.cmd.parser.PingParser;
import com.moilioncircle.redis.replicator.cmd.parser.ReplConfParser;
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
    private static final Option REPLACE = Option.builder("r").longOpt("replace").required(false).desc("Replace exist key value. If not specified, default value is false.").build();
    private static final Option MIGRATE = Option.builder("m").longOpt("migrate").required(false).hasArg().argName("uri").type(String.class).desc("Migrate to uri. eg: redis://host:port?authPassword=foobar.").build();
    private static final Option DB = Option.builder("d").longOpt("db").required(false).hasArg().argName("num num...").valueSeparator(' ').type(Number.class).desc("Database Number. Multiple databases can be provided. If not specified, all databases will be included.").build();
    private static final Option KEY = Option.builder("k").longOpt("key").required(false).hasArg().argName("regex regex...").valueSeparator(' ').desc("Keys to export. This can be a RegEx. If not specified, all keys will be returned.").build();
    private static final Option TYPE = Option.builder("t").longOpt("type").required(false).hasArgs().argName("type type...").valueSeparator(' ').desc("Data type to include. Possible values are string, hash, set, sortedset, list, module, stream. Multiple types can be provided. If not specified, all data types will be returned.").build();
    
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
                writeLine("Missing required options: " + sb.toString() + ", `rmt -h` for more information.");
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
            dress(r, migrate, db, regexs, Type.parse(type), replace);
            r.open();
        }
    }
    
    private void dress(Replicator r, String migrate, List<Long> db, List<String> regexs, List<Type> types, boolean replace) throws Exception {
        r.setRdbVisitor(new MigrateRdbVisitor(r, migrate, db, regexs, types, replace));
        // ignore PING REPLCONF GETACK
        r.addCommandParser(CommandName.name("PING"), new PingParser());
        r.addCommandParser(CommandName.name("REPLCONF"), new ReplConfParser());
        //
        r.addCommandParser(CommandName.name("APPEND"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SET"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SETEX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("MSET"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("DEL"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SADD"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("HMSET"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("HSET"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LSET"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("EXPIRE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("EXPIREAT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("GETSET"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("HSETNX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("MSETNX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PSETEX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SETNX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SETRANGE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("HDEL"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LPOP"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LPUSH"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LPUSHX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LRem"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RPOP"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RPUSH"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RPUSHX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZREM"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RENAME"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("INCR"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("DECR"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("INCRBY"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("DECRBY"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PERSIST"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SELECT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("FLUSHALL"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("FLUSHDB"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("HINCRBY"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZINCRBY"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("MOVE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SMOVE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PFADD"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PFCOUNT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PFMERGE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SDIFFSTORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SINTERSTORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SUNIONSTORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZADD"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZINTERSTORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZUNIONSTORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("BRPOPLPUSH"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LINSERT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RENAMENX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RESTORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PEXPIRE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PEXPIREAT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("GEOADD"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("EVAL"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("EVALSHA"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SCRIPT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("PUBLISH"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("BITOP"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("BITFIELD"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SETBIT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SREM"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("UNLINK"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SWAPDB"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("MULTI"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("EXEC"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZREMRANGEBYSCORE"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZREMRANGEBYRANK"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZREMRANGEBYLEX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("LTRIM"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("SORT"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("RPOPLPUSH"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZPOPMIN"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("ZPOPMAX"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("XACK"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("XADD"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("XCLAIM"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("XDEL"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("XGROUP"), new DefaultCommandParser());
        r.addCommandParser(CommandName.name("XTRIM"), new DefaultCommandParser());
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
