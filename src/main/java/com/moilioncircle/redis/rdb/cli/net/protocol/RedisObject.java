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

package com.moilioncircle.redis.rdb.cli.net.protocol;

/**
 * @author Baoyi Chen
 */
public class RedisObject {
    public Type type;
    public Object object;

    public RedisObject(Type type, Object object) {
        this.type = type;
        this.object = object;
    }

    public String getString() {
        byte[] b = getBytes();
        return b == null ? null : new String(b);
    }

    public byte[] getBytes() {
        if (type.isString() || type.isError()) {
            byte[] bytes = (byte[]) object;
            return bytes;
        }
        return null;
    }

    public RedisObject[] getArray() {
        if (type.isArray()) {
            RedisObject[] array = (RedisObject[]) object;
            return array;
        }
        return null;
    }

    public Long getNumber() {
        if (type.isNumber()) {
            return (Long)object;
        }
        return null;
    }

    public enum Type {
        ARRAY, NUMBER, STRING, BULK, ERR, NULL;

        public boolean isString() {
            return this == BULK || this == STRING;
        }

        public boolean isArray() {
            return this == ARRAY;
        }

        public boolean isNumber() {
            return this == NUMBER;
        }

        public boolean isError() {
            return this == ERR;
        }

        public boolean isNull() {
            return this == NULL;
        }
    }
}
