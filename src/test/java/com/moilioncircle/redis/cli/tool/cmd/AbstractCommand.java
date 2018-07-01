package com.moilioncircle.redis.cli.tool.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintWriter;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractCommand implements Command {
    
    protected Options options = new Options();
    protected PrintWriter pw = new PrintWriter(System.out);
    
    protected abstract void doExecute(CommandLine line) throws Exception;
    
    public void addOption(Option option) {
        options.addOption(option);
    }
    
    public void addOptionGroup(OptionGroup group) {
        options.addOptionGroup(group);
    }
    
    public void execute(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            doExecute(line);
        } catch (ParseException e) {
            e.printStackTrace();
            pw.println(e.getMessage());
        }
    }
}
