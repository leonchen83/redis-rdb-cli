package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.replicator.Replicator;

import java.io.File;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class JsonRdbVisitor extends BaseRdbVisitor {
    public JsonRdbVisitor(Replicator replicator, File out, Long db, String keyRegEx, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, keyRegEx, top, types, escape);
    }
}