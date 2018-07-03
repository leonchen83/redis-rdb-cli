package com.moilioncircle.redis.cli.tool.ext.datatype;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

/**
 * @author Baoyi Chen
 */
public class DummyKeyValuePair extends KeyValuePair<byte[], byte[]> {
    private static final long serialVersionUID = 1L;

    private boolean contains;

    public boolean isContains() {
        return contains;
    }

    public void setContains(boolean contains) {
        this.contains = contains;
    }
}
