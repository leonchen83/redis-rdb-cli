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

package com.moilioncircle.redis.cli.tool.cmd;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.TypeHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class CommandLine {
    
    private org.apache.commons.cli.CommandLine line;
    
    public CommandLine(org.apache.commons.cli.CommandLine line) {
        this.line = line;
    }
    
    public boolean hasOption(String opt) {
        return line.hasOption(opt);
    }
    
    public <T> List<T> getOptions(String opt) throws ParseException {
        String[] res = line.getOptionValues(opt);
        Option option = resolveOption(opt);
        if (option == null || res == null) {
            return new ArrayList<>();
        }
        
        Class<?> clazz = (Class<?>) option.getType();
        List<T> list = new ArrayList<>();
        for (int i = 0; i < res.length; i++) {
            T val = getOption(res[i], clazz);
            list.add(val);
        }
        return list;
    }
    
    public <T> T getOption(String opt) throws ParseException {
        List<T> list = getOptions(opt);
        if (list.isEmpty()) return null;
        return list.get(0);
    }
    
    private <T> T getOption(String res, Class<?> clazz) throws ParseException {
        return (T) TypeHandler.createValue(res, clazz);
    }
    
    private Option resolveOption(String opt) {
        opt = stripLeadingHyphens(opt);
        for (Option option : line.getOptions()) {
            if (opt.equals(option.getOpt())) {
                return option;
            }
            
            if (opt.equals(option.getLongOpt())) {
                return option;
            }
            
        }
        return null;
    }
    
    private static String stripLeadingHyphens(String str) {
        if (str == null) {
            return null;
        }
        if (str.startsWith("--")) {
            return str.substring(2, str.length());
        } else if (str.startsWith("-")) {
            return str.substring(1, str.length());
        }
        
        return str;
    }
}
