package com.moilioncircle.redis.cli.tool.cmd;

/**
 * @author Baoyi Chen
 */
public class RmtCommand extends AbstractCommand {

    @Override
    protected void doExecute(CommandLine line) throws Exception {

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
