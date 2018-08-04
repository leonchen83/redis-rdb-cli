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
