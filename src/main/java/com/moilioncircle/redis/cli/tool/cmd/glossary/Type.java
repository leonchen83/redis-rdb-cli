package com.moilioncircle.redis.cli.tool.cmd.glossary;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Baoyi Chen
 */
public enum Type {

    ALL("all"),
    SET("set"),
    LIST("list"),
    HASH("hash"),
    STRING("string"),
    MODULE("module"),
    STREAM("stream"),
    SORTEDSET("sortedset");

    private String value;

    Type(String value) {
        this.value = value;
    }

    public static List<Type> parse(List<String> list) {
        if (list.isEmpty()) return asList(ALL);
        List<Type> r = new ArrayList<>(list.size());
        for (String name : list) r.add(Type.valueOf(name));
        return r;
    }

    public static boolean contains(List<Type> list, int rdb) {
        if (list.isEmpty()) return true;
        for (Type type : list) if (type.contains(rdb)) return true;
        return false;
    }

    public boolean contains(int rdbType) {
        switch (this) {
            case ALL:
                return true;
            case STRING:
                return rdbType == 0;
            case SET:
                return rdbType == 2 || rdbType == 11;
            case HASH:
                return rdbType == 4 || rdbType == 9 || rdbType == 13;
            case LIST:
                return rdbType == 1 || rdbType == 10 || rdbType == 14;
            case SORTEDSET:
                return rdbType == 3 || rdbType == 5 || rdbType == 12;
            case MODULE:
                return rdbType == 6 || rdbType == 7;
            case STREAM:
                return rdbType == 15;
        }
        return false;
    }

}
