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

package com.moilioncircle.redis.rdb.cli.glossary;

/**
 * @author Baoyi Chen
 */
public enum MonitorType {
    GAUGE(1), COUNTER(2);
    private int value;

    MonitorType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
    
    public static MonitorType valueOf(int value) {
        switch (value) {
            case 1 : return GAUGE;
            case 2 : return COUNTER;
            default:
                throw new UnsupportedOperationException(String.valueOf(value));
        }
    }
}
