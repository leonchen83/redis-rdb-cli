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

import org.apache.commons.cli.Option;

/**
 * @author Baoyi Chen
 */
public interface Command {
    
    String name();

    void addOption(Option option);
    
    void execute(String[] args) throws Exception;

    void write(String message) throws Exception;

    void writeLine(String message) throws Exception;

    void writeError(String message) throws Exception;
}
