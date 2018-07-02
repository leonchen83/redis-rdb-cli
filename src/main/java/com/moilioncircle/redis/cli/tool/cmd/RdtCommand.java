package com.moilioncircle.redis.cli.tool.cmd;

/**
 * @author Baoyi Chen
 */
public class RdtCommand extends AbstractCommand {

    @Override
    protected void doExecute(CommandLine line) throws Exception {

    }

    @Override
    public String name() {
        return "rdt";
    }

    public static void run(String[] args) throws Exception {
        RdtCommand command = new RdtCommand();
        command.execute(args);
    }
}
