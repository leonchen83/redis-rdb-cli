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

package com.moilioncircle.redis.rdb.cli.ext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.sentinel.RedisSentinelReplicator;
import com.moilioncircle.redis.rdb.cli.sentinel.RedisSentinelURI;
import com.moilioncircle.redis.replicator.CloseListener;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.ExceptionListener;
import com.moilioncircle.redis.replicator.RedisAofReplicator;
import com.moilioncircle.redis.replicator.RedisMixReplicator;
import com.moilioncircle.redis.replicator.RedisRdbReplicator;
import com.moilioncircle.redis.replicator.RedisScanReplicator;
import com.moilioncircle.redis.replicator.RedisSocketReplicator;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.ReplFilter;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.Status;
import com.moilioncircle.redis.replicator.StatusListener;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.CommandParser;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.io.PeekableInputStream;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.rdb.RdbVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;

/**
 * @author Baoyi Chen
 */
public class XRedisReplicator implements Replicator {

    protected Replicator replicator;

    public XRedisReplicator(String uri, Configure configure, ReplFilter... filters) throws URISyntaxException, IOException {
        Objects.requireNonNull(uri);
        try {
            RedisURI u = new RedisURI(uri);
            initialize(u, configure, filters);
        } catch (URISyntaxException e) {
            RedisSentinelURI u = new RedisSentinelURI(uri);
            initialize(u, configure, filters);
        }
    }
    
    public XRedisReplicator(RedisURI uri, Configure configure, ReplFilter... filters) throws URISyntaxException, IOException {
        Objects.requireNonNull(uri);
        initialize(uri, configure, filters);
    }
    
    public XRedisReplicator(RedisSentinelURI uri, Configure configure, ReplFilter... filters) throws URISyntaxException, IOException {
        Objects.requireNonNull(uri);
        initialize(uri, configure, filters);
    }

    private void initialize(RedisURI uri, Configure configure, ReplFilter... filters) throws IOException {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(configure);
        Configuration configuration = configure.merge(uri, true);
        configuration.setReplFilters(filters);
        if (uri.getFileType() != null) {
            PeekableInputStream in = new PeekableInputStream(uri.toURL().openStream());
            switch (uri.getFileType()) {
                case AOF:
                    if (in.peek() == 'R') {
                        this.replicator = new RedisMixReplicator(in, configuration);
                    } else {
                        this.replicator = new RedisAofReplicator(in, configuration);
                    }
                    break;
                case RDB:
                    this.replicator = new RedisRdbReplicator(in, configuration);
                    break;
                case MIXED:
                    this.replicator = new RedisMixReplicator(in, configuration);
                    break;
                default:
                    throw new UnsupportedOperationException(uri.getFileType().toString());
            }
        } else {
            if (configuration.isEnableScan()) {
                this.replicator = new RedisScanReplicator(uri.getHost(), uri.getPort(), configuration);
            } else {
                this.replicator = new RedisSocketReplicator(uri.getHost(), uri.getPort(), configuration);
            }
        }
        this.replicator.addExceptionListener((r, t, e) -> {
            throw new RuntimeException(t.getMessage(), t);
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Replicators.closeQuietly(this.replicator);
        }));
    }

    private void initialize(RedisSentinelURI uri, Configure configure, ReplFilter... filters) throws IOException {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(configure);
        Configuration configuration = configure.merge(uri, true);
        configuration.setReplFilters(filters);
        this.replicator = new RedisSentinelReplicator(uri, configuration);
        this.replicator.addExceptionListener((r, t, e) -> {
            throw new RuntimeException(t.getMessage(), t);
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Replicators.closeQuietly(this.replicator);
        }));
    }

    @Override
    public boolean addRawByteListener(RawByteListener listener) {
        return replicator.addRawByteListener(listener);
    }

    @Override
    public boolean removeRawByteListener(RawByteListener listener) {
        return replicator.removeRawByteListener(listener);
    }

    @Override
    public void builtInCommandParserRegister() {
        replicator.builtInCommandParserRegister();
    }

    @Override
    public CommandParser<? extends Command> getCommandParser(CommandName command) {
        return replicator.getCommandParser(command);
    }

    @Override
    public <T extends Command> void addCommandParser(CommandName command, CommandParser<T> parser) {
        replicator.addCommandParser(command, parser);
    }

    @Override
    public CommandParser<? extends Command> removeCommandParser(CommandName command) {
        return replicator.removeCommandParser(command);
    }

    @Override
    public ModuleParser<? extends Module> getModuleParser(String moduleName, int moduleVersion) {
        return replicator.getModuleParser(moduleName, moduleVersion);
    }

    @Override
    public <T extends Module> void addModuleParser(String moduleName, int moduleVersion, ModuleParser<T> parser) {
        replicator.addModuleParser(moduleName, moduleVersion, parser);
    }

    @Override
    public ModuleParser<? extends Module> removeModuleParser(String moduleName, int moduleVersion) {
        return replicator.removeModuleParser(moduleName, moduleVersion);
    }

    @Override
    public void setRdbVisitor(RdbVisitor rdbVisitor) {
        replicator.setRdbVisitor(rdbVisitor);
    }

    @Override
    public RdbVisitor getRdbVisitor() {
        return replicator.getRdbVisitor();
    }

    @Override
    public boolean addEventListener(EventListener listener) {
        return replicator.addEventListener(listener);
    }

    @Override
    public boolean removeEventListener(EventListener listener) {
        return replicator.removeEventListener(listener);
    }

    @Override
    public boolean addCloseListener(CloseListener listener) {
        return replicator.addCloseListener(listener);
    }

    @Override
    public boolean removeCloseListener(CloseListener listener) {
        return replicator.removeCloseListener(listener);
    }

    @Override
    public boolean addExceptionListener(ExceptionListener listener) {
        return replicator.addExceptionListener(listener);
    }

    @Override
    public boolean removeExceptionListener(ExceptionListener listener) {
        return replicator.removeExceptionListener(listener);
    }

    @Override
    public boolean addStatusListener(StatusListener listener) {
        return replicator.addStatusListener(listener);
    }

    @Override
    public boolean removeStatusListener(StatusListener listener) {
        return replicator.removeStatusListener(listener);
    }

    @Override
    public boolean verbose() {
        return replicator.verbose();
    }

    @Override
    public Status getStatus() {
        return replicator.getStatus();
    }

    @Override
    public Configuration getConfiguration() {
        return replicator.getConfiguration();
    }

    @Override
    public void open() throws IOException {
        replicator.open();
    }

    @Override
    public void close() throws IOException {
        replicator.close();
    }
}
