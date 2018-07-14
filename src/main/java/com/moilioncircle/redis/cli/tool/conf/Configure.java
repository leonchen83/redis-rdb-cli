package com.moilioncircle.redis.cli.tool.conf;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.UncheckedIOException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Baoyi Chen
 */
public class Configure {
    
    private Properties properties;
    
    private Configure() {
        this.properties = new Properties();
        try {
            String path = System.getProperty("conf");
            if (path != null && path.trim().length() != 0) {
                try (InputStream in = new FileInputStream(path)) {
                    properties.load(in);
                }
            } else {
                ClassLoader loader = Configure.class.getClassLoader();
                try (InputStream in = loader.getResourceAsStream("cli.conf")) {
                    properties.load(in);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    private Configure(Properties properties) {
        this();
        if (properties != null)
            this.properties.putAll(properties);
    }
    
    /**
     * --format resp batch size
     */
    private int batchSize = 128;
    
    /**
     * --format dump replace
     */
    private boolean dumpReplace = true;
    
    /**
     * rct quote
     */
    private byte quote = '"';
    
    /**
     * rct delimiter
     */
    private byte delimiter = ',';
    
    /**
     * timeout
     */
    private int timeout = 30000;
    
    /**
     * socket receive buffer size
     */
    private int rcvBuf = 0;
    
    /**
     * socket send buffer size
     */
    private int sndBuf = 0;
    
    /**
     * connection retry times. if retries <= 0 then always retry
     */
    private int retryTime = 5;
    
    /**
     * rmt --migrate
     */
    private int migrateRetryTime = 1;
    
    /**
     * retry time interval
     */
    private int retryInterval = 1000;
    
    /**
     * redis input stream buffer size
     */
    private int bufferSize = 8 * 1024;
    
    /**
     * async buffer size
     */
    private int asyncCacheSize = 512 * 1024;
    
    /**
     *
     */
    private int dumpRdbVersion = -1;
    
    /**
     * trace event log
     */
    private boolean verbose = false;
    
    /**
     * used in psync heartbeat
     */
    private int heartbeat = 1000;
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public boolean isDumpReplace() {
        return dumpReplace;
    }
    
    public void setDumpReplace(boolean dumpReplace) {
        this.dumpReplace = dumpReplace;
    }
    
    public byte getQuote() {
        return quote;
    }
    
    public void setQuote(byte quote) {
        this.quote = quote;
    }
    
    public byte getDelimiter() {
        return delimiter;
    }
    
    public void setDelimiter(byte delimiter) {
        this.delimiter = delimiter;
    }
    
    public int getTimeout() {
        return timeout;
    }
    
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
    public int getRcvBuf() {
        return rcvBuf;
    }
    
    public void setRcvBuf(int rcvBuf) {
        this.rcvBuf = rcvBuf;
    }
    
    public int getSndBuf() {
        return sndBuf;
    }
    
    public void setSndBuf(int sndBuf) {
        this.sndBuf = sndBuf;
    }
    
    public int getRetryTime() {
        return retryTime;
    }
    
    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }
    
    public int getMigrateRetryTime() {
        return migrateRetryTime;
    }
    
    public void setMigrateRetryTime(int migrateRetryTime) {
        this.migrateRetryTime = migrateRetryTime;
    }
    
    public int getRetryInterval() {
        return retryInterval;
    }
    
    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }
    
    public int getBufferSize() {
        return bufferSize;
    }
    
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
    
    public int getAsyncCacheSize() {
        return asyncCacheSize;
    }
    
    public void setAsyncCacheSize(int asyncCacheSize) {
        this.asyncCacheSize = asyncCacheSize;
    }
    
    public int getDumpRdbVersion() {
        return dumpRdbVersion;
    }
    
    public void setDumpRdbVersion(int dumpRdbVersion) {
        this.dumpRdbVersion = dumpRdbVersion;
    }
    
    public boolean isVerbose() {
        return verbose;
    }
    
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
    
    public int getHeartbeat() {
        return heartbeat;
    }
    
    public void setHeartbeat(int heartbeat) {
        this.heartbeat = heartbeat;
    }
    
    public Configuration merge(RedisURI uri) {
        return merge(Configuration.valueOf(uri));
    }
    
    public Configuration merge(Configuration conf) {
        conf.setRetries(this.retryTime);
        conf.setRetryTimeInterval(this.retryInterval);
        conf.setConnectionTimeout(this.timeout);
        conf.setReadTimeout(this.timeout);
        conf.setSendBufferSize(this.sndBuf);
        conf.setReceiveBufferSize(this.rcvBuf);
        conf.setBufferSize(this.bufferSize);
        conf.setAsyncCachedBytes(this.asyncCacheSize);
        conf.setVerbose(this.verbose);
        conf.setHeartBeatPeriod(this.heartbeat);
        return conf;
    }
    
    public static Configure bind() {
        return bind(null);
    }
    
    public static Configure bind(Properties properties) {
        Configure conf = new Configure(properties);
        conf.batchSize = getInt(conf, "batch_size", 128, true);
        conf.migrateRetryTime = getInt(conf, "migrate_retry_time", 1, true);
        conf.dumpRdbVersion = getInt(conf, "dump_rdb_version", -1, true);
        conf.dumpReplace = getBool(conf, "dump_replace", true, true);
        conf.quote = (byte) getString(conf, "quote", "\"", true).charAt(0);
        conf.delimiter = (byte) getString(conf, "delimiter", ",", true).charAt(0);
        conf.retryTime = getInt(conf, "retry_time", 5, true);
        conf.retryInterval = getInt(conf, "retry_interval", 1000, true);
        conf.timeout = getInt(conf, "timeout", 30000, true);
        conf.sndBuf = getInt(conf, "snd_buf", 0, true);
        conf.rcvBuf = getInt(conf, "rcv_buf", 0, true);
        conf.bufferSize = getInt(conf, "buffer_size", 8 * 1024, true);
        conf.asyncCacheSize = getInt(conf, "async_cache_size", 512 * 1024, true);
        conf.verbose = getBool(conf, "verbose", false, true);
        conf.heartbeat = getInt(conf, "heartbeat", 1000, true);
        return conf;
    }
    
    public static String getString(Configure conf, String key) {
        return getString(conf, key, null, false);
    }
    
    public static Integer getInt(Configure conf, String key) {
        return getInt(conf, key, null, false);
    }
    
    public static Boolean getBool(Configure conf, String key) {
        return getBool(conf, key, null, false);
    }
    
    public static String getString(Configure conf, String key, String value, boolean optional) {
        String v = System.getProperty(key);
        if (v == null && (v = conf.properties.getProperty(key)) == null)
            v = value;
        if (v == null && !optional) {
            throw new IllegalArgumentException("not found the config[key=" + key + "]");
        }
        return v;
    }
    
    public static Integer getInt(Configure conf, String key, Integer value, boolean optional) {
        String v = getString(conf, key, value == null ? null : value.toString(), optional);
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not found the config[key=" + key + "]");
        }
    }
    
    public static Boolean getBool(Configure conf, String key, Boolean value, boolean optional) {
        String v = getString(conf, key, value == null ? null : value.toString(), optional);
        if (v == null)
            return value;
        if (v.equals("yes") || v.equals("true"))
            return Boolean.TRUE;
        if (v.equals("no") || v.equals("false"))
            return Boolean.FALSE;
        throw new IllegalArgumentException("not found the config[key=" + key + "]");
    }
    
    @Override
    public String toString() {
        return "Configure{" +
                "batchSize=" + batchSize +
                ", dumpReplace=" + dumpReplace +
                ", quote=" + quote +
                ", delimiter=" + delimiter +
                ", timeout=" + timeout +
                ", rcvBuf=" + rcvBuf +
                ", sndBuf=" + sndBuf +
                ", retryTime=" + retryTime +
                ", migrateRetryTime=" + migrateRetryTime +
                ", retryInterval=" + retryInterval +
                ", bufferSize=" + bufferSize +
                ", asyncCacheSize=" + asyncCacheSize +
                ", dumpRdbVersion=" + dumpRdbVersion +
                ", verbose=" + verbose +
                ", heartbeat=" + heartbeat +
                '}';
    }
}
