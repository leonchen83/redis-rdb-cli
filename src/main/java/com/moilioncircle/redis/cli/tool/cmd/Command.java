package com.moilioncircle.redis.cli.tool.cmd;

import org.apache.commons.cli.Option;

/**
 * @author Baoyi Chen
 */
public interface Command {
    
    String name();
    
    void addOption(Option option);
    
    void execute(String[] args) throws Exception;
}
