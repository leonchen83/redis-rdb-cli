package com.moilioncircle.redis.cli.tool.ext.datatype;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

/**
 * @author Baoyi Chen
 */
public class DummyKeyValuePair extends KeyValuePair<byte[], Long> {
    private static final long serialVersionUID = 1L;
    
    private long max;
    
    private long length;
    
    private boolean contains;
    
    public long getMax() {
        return max;
    }
    
    public void setMax(long max) {
        this.max = max;
    }
    
    public long getLength() {
        return length;
    }
    
    public void setLength(long length) {
        this.length = length;
    }
    
    public boolean isContains() {
        return contains;
    }
    
    public void setContains(boolean contains) {
        this.contains = contains;
    }
    
}
