package com.moilioncircle.redis.cli.tool.cmd.glossary;

/**
 * @author Baoyi Chen
 */
public enum Escape {
    RAW("raw"),
    PRINT("print"),
    BASE64("base64"),
    UTF8("utf8");

    private String value;

    Escape(String value) {
        this.value = value;
    }

    public static Escape parse(String escape) {
        if (escape == null) return RAW;
        return Escape.valueOf(escape);
    }
}
