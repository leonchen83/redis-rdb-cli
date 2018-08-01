package com.moilioncircle.redis.rdb.cli.glossary;

/**
 * @author Baoyi Chen
 */
public enum Gateway {

    LOG("log"),
    PROMETHEUS("prometheus");

    private String value;

    Gateway(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}
