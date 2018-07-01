package com.moilioncircle.redis.cli.tool;

import com.moilioncircle.redis.cli.tool.cmd.RCTCommand;

/**
 * @author Baoyi Chen
 */
public class Main {
    public static void main(String[] args) throws Exception {
        RCTCommand command = new RCTCommand();
        command.execute(args);
    }
}
