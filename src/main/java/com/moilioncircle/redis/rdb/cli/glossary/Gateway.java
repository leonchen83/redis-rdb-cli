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

/**
 * @author Baoyi Chen
 */
public enum Gateway {
    
    LOG("log"),
    NONE("none"),
    PROMETHEUS("prometheus");
    
    private String value;
    
    Gateway(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return this.value;
    }
    
    public static Gateway parse(String value) {
        if (value.equals("log")) return LOG;
        else if (value.equals("none")) return NONE;
        else if (value.equals("prometheus")) return PROMETHEUS;
        else throw new UnsupportedOperationException(value);
    }
}
