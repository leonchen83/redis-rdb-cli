/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.rdb.cli.glossary;

import static com.moilioncircle.redis.replicator.FileType.RDB;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.CliRedisReplicator;
import com.moilioncircle.redis.rdb.cli.ext.rdt.BackupRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rdt.MergeRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rdt.SplitRdbVisitor;
import com.moilioncircle.redis.rdb.cli.io.ShardableFileOutputStream;
import com.moilioncircle.redis.rdb.cli.util.OutputStreams;
import com.moilioncircle.redis.rdb.cli.util.Strings;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public enum Action {
    NONE,
    SPLIT,
    MERGE,
    BACKUP;
    
    @SuppressWarnings("all")
    public List<Tuple2<Replicator, String>> dress(Configure configure, String split, String backup, List<File> merge, String output, List<Long> db, List<String> regexs, File conf, List<DataType> types) throws Exception {
        List<Tuple2<Replicator, String>> list = new ArrayList<>();
        switch (this) {
            case MERGE:
                if (merge.isEmpty()) return list;
                CRCOutputStream out = OutputStreams.newCRCOutputStream(output, configure.getOutputBufferSize());
                int version = 0;
                for (File file : merge) {
                    URI u = file.toURI();
                    RedisURI uri = new RedisURI(new URI("redis", u.getRawAuthority(), u.getRawPath(), u.getRawQuery(), u.getRawFragment()).toString());
                    if (uri.getFileType() == null || uri.getFileType() != RDB) {
                        throw new UnsupportedOperationException("Invalid options: '--merge <file file...>' must be rdb file.");
                    }
                    try (RedisInputStream in = new RedisInputStream(new FileInputStream(file))) {
                        in.skip(5); // skip REDIS
                        version = Math.max(version, Integer.parseInt(in.readString(4)));
                    } catch (EOFException e) {
                        continue;
                    }
                    Replicator r = new CliRedisReplicator(uri.toString(), configure);
                    r.setRdbVisitor(new MergeRdbVisitor(r, configure, db, regexs, types, () -> out));
                    list.add(Tuples.of(r, file.getName()));
                }
                list.get(list.size() - 1).getV1().addCloseListener(r -> {
                    OutputStreams.write(0xFF, out);
                    OutputStreams.write(out.getCRC64(), out);
                    OutputStreams.close(out);
                });
                // header & version
                out.write("REDIS".getBytes());
                out.write(Strings.lappend(version, 4, '0').getBytes());
                return list;
            case SPLIT:
                Replicator r = new CliRedisReplicator(split, configure);
                List<String> lines = Files.readAllLines(conf.toPath());
                r.setRdbVisitor(new SplitRdbVisitor(r, configure, db, regexs, types, () -> new ShardableFileOutputStream(output, lines, configure)));
                list.add(Tuples.of(r, null));
                return list;
            case BACKUP:
                r = new CliRedisReplicator(backup, configure);
                r.setRdbVisitor(new BackupRdbVisitor(r, configure, db, regexs, types, () -> OutputStreams.newCRCOutputStream(output, configure.getOutputBufferSize())));
                list.add(Tuples.of(r, null));
                return list;
            case NONE:
                return list;
            default:
                throw new AssertionError("Unsupported action '" + this + "'");
        }
    }
}
