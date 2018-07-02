package com.moilioncircle.redis.cli.tool.cmd.glossary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public enum Type {

    ALL("all"),
    STRING("string"),
    LIST("list"),
    SET("set"),
    HASH("hash"),
    SORTEDSET("sortedset"),
    MODULE("module"),
    STREAM("stream");

    private String value;

    Type(String value) {
        this.value = value;
    }

    public static List<Type> parse(List<String> list) {
        if (list.isEmpty()) return Arrays.asList(ALL);
        List<Type> r = new ArrayList<>(list.size());
        for (String name : list) {
            r.add(Type.valueOf(name));
        }
        return r;
    }

    public static boolean contains(List<Type> list, int rdbType) {
        for (Type type : list) {
            if (type.contains(rdbType)) return true;
        }
        return false;
    }

    public boolean contains(int rdbType) {
        switch (this) {
            case ALL:
                return true;
            case SET:
                return rdbType == 2 || rdbType == 11;
            case HASH:
                return rdbType == 4 || rdbType == 9 || rdbType == 13;
            case LIST:
                return rdbType == 1 || rdbType == 10 || rdbType == 14;
            case STRING:
                return rdbType == 0;
            case MODULE:
                return rdbType == 6 || rdbType == 7;
            case STREAM:
                return rdbType == 15;
            case SORTEDSET:
                return rdbType == 3 || rdbType == 5 || rdbType == 12;
        }
        return false;
    }

}
