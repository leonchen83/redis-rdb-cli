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

package com.moilioncircle.redis.rdb.cli.cmd;

import com.moilioncircle.redis.rdb.cli.sentinel.RedisSentinelURI;
import com.moilioncircle.redis.rdb.cli.util.Strings;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisURI;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.net.URISyntaxException;

import static com.moilioncircle.redis.rdb.cli.cmd.Version.INSTANCE;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractCommand implements Command {
    
    protected Options options = new Options();
    
    protected abstract void doExecute(CommandLine line) throws Exception;
    
    @Override
    public void addOption(Option option) {
        options.addOption(option);
    }
    
    @Override
    public void execute(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        try {
            org.apache.commons.cli.CommandLine line = parser.parse(options, args);
            doExecute(new CommandLine(line));
        } catch (Throwable e) {
            if (e.getMessage() != null) {
                // https://github.com/leonchen83/redis-rdb-cli/issues/7
                writeError(e.getMessage());
            } else {
                throw e;
            }
        }
    }

    @Override
    public void write(String message) throws Exception {
        System.out.print(message);
        System.out.flush();
    }

    @Override
    public void writeLine(String message) throws Exception {
        System.out.println(message);
    }

    @Override
    public void writeError(String message) throws Exception {
        System.out.println(message);
        System.exit(-1);
    }
    
    protected String normalize(String source, FileType type, String message) throws URISyntaxException {
        try {
            RedisSentinelURI uri = new RedisSentinelURI(source);
            return uri.toString();
        } catch (Throwable e1) {
            RedisURI uri = null;
            try {
                uri = new RedisURI(source);
            } catch (Throwable e2) {
                try {
                    uri = new RedisURI(new File(source));
                } catch (Throwable e3) {
                }
            }
            if (uri != null && (uri.getFileType() == null || type == null || uri.getFileType() == type)) {
                return uri.toString();
            }
        }
        throw new AssertionError(message);
    }

    protected String version() {
        StringBuilder builder = new StringBuilder();
        builder.append("redis rdb cli: ");
        if (INSTANCE.version() != null) {
            builder.append(INSTANCE.version());
        }
        if (!Strings.isEmpty(INSTANCE.commit())) {
            builder.append(" (").append(INSTANCE.commit()).append(": ");
            builder.append(INSTANCE.date()).append(")");
        }
        builder.append("\n");
        if (!Strings.isEmpty(INSTANCE.home())) {
            builder.append("rct home: ").append(INSTANCE.home()).append("\n");
        }
        builder.append("java version: ").append(System.getProperty("java.version")).append(", ");
        builder.append("vendor: ").append(System.getProperty("java.vendor")).append("\n");
        builder.append("java home: ").append(System.getProperty("java.home")).append("\n");
        builder.append("default locale: ").append(System.getProperty("user.language")).append(", ");
        builder.append("platform encoding: ").append(System.getProperty("file.encoding")).append("\n");
        builder.append("os name: ").append(System.getProperty("os.name")).append(", ");
        builder.append("version: ").append(System.getProperty("os.version")).append(", ");
        builder.append("arch: ").append(System.getProperty("os.arch"));
        return builder.toString();
    }
}
