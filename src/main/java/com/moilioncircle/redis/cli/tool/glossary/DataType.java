package com.moilioncircle.redis.cli.tool.glossary;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * @author Baoyi Chen
 */
public enum DataType {

    ALL("all"),
    SET("set"),
    LIST("list"),
    HASH("hash"),
    STRING("string"),
    MODULE("module"),
    STREAM("stream"),
    SORTEDSET("sortedset");

    private String value;

    DataType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static List<DataType> parse(List<String> list) {
        if (list.isEmpty()) return singletonList(ALL);
        List<DataType> r = new ArrayList<>(list.size());
        for (String name : list) r.add(DataType.parse(name));
        return r;
    }

    public static boolean contains(List<DataType> list, int rdb) {
        if (list.isEmpty()) return true;
        for (DataType type : list) if (type.contains(rdb)) return true;
        return false;
    }

    public static DataType parse(String type) {
        switch (type) {
            case "all":
                return ALL;
            case "set":
                return SET;
            case "list":
                return LIST;
            case "hash":
                return HASH;
            case "string":
                return STRING;
            case "module":
                return MODULE;
            case "stream":
                return STREAM;
            case "sortedset":
                return SORTEDSET;
            default:
                throw new AssertionError("Unsupported type '" + type + "'");

        }
    }

    public static DataType parse(int type) {
        switch (type) {
            case 0:
                return STRING;
            case 2:
            case 11:
                return SET;
            case 4:
            case 9:
            case 13:
                return HASH;
            case 1:
            case 10:
            case 14:
                return LIST;
            case 3:
            case 5:
            case 12:
                return SORTEDSET;
            case 6:
            case 7:
                return MODULE;
            case 15:
                return STREAM;
            default:
                throw new AssertionError(type);
        }
    }

    public boolean contains(int type) {
        switch (this) {
            case ALL:
                return true;
            case STRING:
                return type == 0;
            case SET:
                return type == 2 || type == 11;
            case HASH:
                return type == 4 || type == 9 || type == 13;
            case LIST:
                return type == 1 || type == 10 || type == 14;
            case SORTEDSET:
                return type == 3 || type == 5 || type == 12;
            case MODULE:
                return type == 6 || type == 7;
            case STREAM:
                return type == 15;
        }
        return false;
    }

    public static String type(int type) {
        switch (type) {
            case 0:
                return "string";
            case 2:
                return "hash";
            case 11:
                return "intset";
            case 4:
                return "hash";
            case 9:
                return "zipmap";
            case 13:
                return "ziplist";
            case 1:
                return "list";
            case 10:
                return "ziplist";
            case 14:
                return "quicklist";
            case 3:
                return "skiplist";
            case 5:
                return "skiplist";
            case 12:
                return "ziplist";
            case 6:
                return "module";
            case 7:
                return "module2";
            case 15:
                return "listpacks";
            default:
                throw new AssertionError(type);
        }
    }
}
