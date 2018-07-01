package com.moilioncircle.redis.cli.tool.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;

/**
 * @author Baoyi Chen
 */
public class RCTCommand extends AbstractCommand {
    
    private static final Option HELP = Option.builder("h").longOpt("help").required(false).hasArg(false).desc("rct usage.").build();
    private static final Option VERSION = Option.builder("v").longOpt("version").required(false).hasArg(false).desc("rct version.").build();
    private static final Option FORMAT = Option.builder("f").longOpt("format").required(true).hasArg().argName("FORMAT").desc("Command to execute. Valid commands are json, dump, key, val, mem and resp").build();
    private static final Option INPUT = Option.builder("i").longOpt("in").required(true).hasArg().argName("RESOURCE").desc("Input file").build();
    private static final Option OUTPUT = Option.builder("o").longOpt("out").required(true).hasArg().argName("RESOURCE").desc("Output file").build();
    private static final Option DBS = Option.builder("d").longOpt("db").required(false).hasArg().argName("DB").desc("Database Number. Multiple databases can be provided. If not specified, all databases will be included.").build();
    private static final Option KEYS = Option.builder("k").longOpt("key").required(false).hasArg().argName("KEYS").desc("Keys to export. This can be a regular expression.").build();
    private static final Option NOT_KEYS = Option.builder("n").longOpt("not-key").required(false).hasArg().argName("NOT_KEYS").desc("Keys Not to export. This can be a regular expression").build();
    private static final Option TYPES = Option.builder("t").longOpt("types").required(false).hasArg().argName("TYPES").desc("Data types to include. Possible values are string, hash, set, sortedset, list. Multiple typees can be provided. If not specified, all data types will be returned").build();
    private static final Option BYTES = Option.builder("b").longOpt("bytes").required(false).hasArg().argName("BYTES").desc("Limit memory output to keys greater to or equal to this value (in bytes)").build();
    private static final Option LARGEST = Option.builder("l").longOpt("largest").required(false).hasArg().argName("LARGEST").desc("Limit memory output to only the top N keys (by size)").build();
    private static final Option ESCAPE = Option.builder("e").longOpt("escape").required(false).hasArg().argName("ESCAPE").desc("Escape strings to encoding: raw bytes (default), print, utf8, or base64.").build();
    
    @Override
    public String name() {
        return "rct";
    }
    
    public RCTCommand() {
        addOption(HELP);
        addOption(VERSION);
        OptionGroup group = new OptionGroup();
        group.setRequired(false);
        group.addOption(FORMAT);
        group.addOption(INPUT);
        group.addOption(OUTPUT);
        group.addOption(DBS);
        group.addOption(KEYS);
        group.addOption(NOT_KEYS);
        group.addOption(TYPES);
        group.addOption(BYTES);
        group.addOption(LARGEST);
        group.addOption(ESCAPE);
        addOptionGroup(group);
    }
    
    @Override
    protected void doExecute(CommandLine line) throws Exception {
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("rct", options);
        } else if (line.hasOption("version")) {
            pw.println("1.0.0");
        }
    }
}
