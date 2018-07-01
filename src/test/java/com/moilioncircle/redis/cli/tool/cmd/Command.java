package com.moilioncircle.redis.cli.tool.cmd;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;

/**
 * @author Baoyi Chen
 */
public interface Command {
    
    String name();
    
    void addOption(Option option);
    
    void addOptionGroup(OptionGroup group);
    
    void execute(String[] args) throws Exception;
    
}
