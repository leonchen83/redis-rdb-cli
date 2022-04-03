/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli.ext.datatype;

import java.nio.ByteBuffer;

/**
 * @author Baoyi Chen
 */
public class CommandConstants {
    
    public static final byte[] ZERO = "0".getBytes();
    public static final byte[] ONE = "1".getBytes();
    public static final byte[] TWO = "2".getBytes();
    public static final byte[] THREE = "3".getBytes();
    public static final byte[] FOUR = "4".getBytes();
    public static final byte[] FIVE = "5".getBytes();
    
    public static final byte[] AUTH = "auth".getBytes();
    public static final byte[] PING = "ping".getBytes();
    public static final byte[] ROLE = "role".getBytes();
    public static final byte[] DEL = "del".getBytes();
    
    public static final byte[] SET = "set".getBytes();
    public static final byte[] SADD = "sadd".getBytes();
    public static final byte[] ZADD = "zadd".getBytes();
    public static final byte[] LOAD = "load".getBytes();
    public static final byte[] RPUSH = "rpush".getBytes();
    public static final byte[] HMSET = "hmset".getBytes();
    
    public static final byte[] SELECT = "select".getBytes();
    public static final byte[] REPLACE = "replace".getBytes();
    
    public static final byte[] SCRIPT = "script".getBytes();
    public static final byte[] EVALSHA = "evalsha".getBytes();
    
    public static final byte[] DELETE = "delete".getBytes();
    public static final byte[] FUNCTION = "function".getBytes();
    public static final byte[] DESCRIPTION = "description".getBytes();
    
    public static final byte[] EXPIREAT = "expireat".getBytes();
    public static final byte[] PEXPIREAT = "pexpireat".getBytes();
    
    public static final byte[] RESTORE = "restore".getBytes();
    public static final byte[] RESTORE_ASKING = "restore-asking".getBytes();
    
    public static final byte[] CLUSTER = "cluster".getBytes();
    public static final byte[] NODES = "nodes".getBytes();
    
    // monitor
    public static final byte[] INFO = "info".getBytes();
    public static final byte[] ALL = "all".getBytes();
    public static final byte[] CONFIG = "config".getBytes();
    public static final byte[] GET = "get".getBytes();
    public static final byte[] SLOWLOG = "slowlog".getBytes();
    public static final byte[] LEN = "len".getBytes();
    public static final byte[] MAXCLIENTS = "maxclients".getBytes();
    
    public static final ByteBuffer ZERO_BUF = ByteBuffer.wrap(ZERO);
    public static final ByteBuffer LOAD_BUF = ByteBuffer.wrap(LOAD);
    public static final ByteBuffer REPLACE_BUF = ByteBuffer.wrap(REPLACE);
    public static final ByteBuffer RESTORE_BUF = ByteBuffer.wrap(RESTORE);
    public static final ByteBuffer FUNCTION_BUF = ByteBuffer.wrap(FUNCTION);
    public static final ByteBuffer DESCRIPTION_BUF = ByteBuffer.wrap(DESCRIPTION);
}
