package com.moilioncircle.redis.cli.tool.glossary;

import com.moilioncircle.redis.cli.tool.conf.Configure;
import com.moilioncircle.redis.cli.tool.ext.CliRedisReplicator;
import com.moilioncircle.redis.cli.tool.ext.rdt.BackupRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rdt.MergeRdbVisitor;
import com.moilioncircle.redis.cli.tool.ext.rdt.SplitRdbVisitor;
import com.moilioncircle.redis.cli.tool.io.FilesOutputStream;
import com.moilioncircle.redis.cli.tool.util.Closes;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.moilioncircle.redis.replicator.FileType.AOF;
import static com.moilioncircle.redis.replicator.FileType.RDB;

/**
 * @author Baoyi Chen
 */
public enum Type {
    NONE,
    SPLIT,
    MERGE,
    BACKUP;
    
    public List<Replicator> dress(Configure configure,
                                  String split,
                                  String backup,
                                  List<File> merge,
                                  String output,
                                  List<Long> db,
                                  List<String> regexs,
                                  String conf,
                                  List<DataType> types) throws Exception {
        switch (this) {
            case MERGE:
                if (merge == null || merge.isEmpty()) return Arrays.asList();
                CRCOutputStream out = new CRCOutputStream(new BufferedOutputStream(new FileOutputStream(output)));
                List<Replicator> list = new ArrayList<>();
                int max = 0;
                for (File file : merge) {
                    try (RedisInputStream in = new RedisInputStream(new FileInputStream(file))) {
                        in.skip(5);
                        max = Math.max(max, Integer.parseInt(in.readString(4)));
                    }
                }
                out.write("REDIS".getBytes());
                out.write((max < 10 ? "000" + max : "00" + max).getBytes());
                for (File file : merge) {
                    URI u = file.toURI();
                    RedisURI mergeUri = new RedisURI(new URI("redis", u.getRawAuthority(), u.getRawPath(), u.getRawQuery(), u.getRawFragment()).toString());
                    if (mergeUri.getFileType() == null || mergeUri.getFileType() != RDB) {
                        throw new UnsupportedOperationException("Invalid options: --merge <file file...> must be a rdb file.");
                    }
                    Replicator r = new CliRedisReplicator(mergeUri, configure);
                    r.setRdbVisitor(new MergeRdbVisitor(r, configure, db, regexs, types, () -> out));
                    list.add(r);
                }
                list.get(list.size() - 1).addCloseListener(r -> {
                    try {
                        out.write(255);
                        out.write(out.getCRC64());
                    } catch (IOException e) {
                    }
                    Closes.closeQuietly(out);
                });
                return list;
            case SPLIT:
                RedisURI splitUri = new RedisURI(backup);
                if (splitUri.getFileType() == AOF) {
                    throw new UnsupportedOperationException("Invalid options: --split <uri> must be 'redis://host:port' or 'redis:///path/to/dump.rdb'.");
                }
                Replicator r = new CliRedisReplicator(split, configure);
                r.setRdbVisitor(new SplitRdbVisitor(r, configure, db, regexs, types, () -> new FilesOutputStream(output, conf)));
                return Arrays.asList(r);
            case BACKUP:
                RedisURI backupUri = new RedisURI(backup);
                if (backupUri.getFileType() != null) {
                    throw new UnsupportedOperationException("Invalid options: --backup <uri> must be 'redis://host:port'.");
                }
                r = new CliRedisReplicator(backup, configure);
                r.setRdbVisitor(new BackupRdbVisitor(r, configure, db, regexs, types, () -> {
                    try {
                        return new CRCOutputStream(new BufferedOutputStream(new FileOutputStream(output)));
                    } catch (FileNotFoundException e) {
                        throw new AssertionError(e);
                    }
                }));
                return Arrays.asList(r);
            case NONE:
                return Arrays.asList();
            default:
                throw new AssertionError(this);
        }
    }
}
