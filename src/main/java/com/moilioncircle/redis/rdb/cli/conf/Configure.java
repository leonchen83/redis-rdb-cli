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

package com.moilioncircle.redis.rdb.cli.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.moilioncircle.redis.rdb.cli.glossary.FileType;
import com.moilioncircle.redis.rdb.cli.glossary.Gateway;
import com.moilioncircle.redis.rdb.cli.sentinel.RedisSentinelURI;
import com.moilioncircle.redis.rdb.cli.util.Strings;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.net.RedisSslContextFactory;

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
                try (InputStream in = loader.getResourceAsStream("redis-rdb-cli.conf")) {
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
    
    public Properties properties() {
        return this.properties;
    }

    /**
     * rct --format resp batch size
     */
    private int batchSize = 128;
    
    /**
     * rmt --migrate
     */
    private int migrateBatchSize = 4096;
    
    /**
     * rmt --migrate
     */
    private int migrateThreads = 4;
    
    /**
     * rmt --migrate
     */
    private int migrateRetries = 1;
    
    /**
     * rmt --migrate
     */
    private boolean migrateFlush = true;

    /**
     * timeout
     */
    private int timeout = 60000;
    
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
    private int retries = 5;
    
    /**
     * retry time interval
     */
    private int retryInterval = 1000;
    
    /**
     * redis input stream buffer size
     */
    private int inputBufferSize = 8 * 1024;
    
    /**
     * redis output stream buffer size
     */
    private int outputBufferSize = 8 * 1024;
    
    /**
     * redis max output stream buffer size
     */
    private int maxOutputBufferSize = 512 * 1024 * 1024;
    
    /**
     * temp file path
     */
    private String tempFilePath = null;
    
    /**
     * temp file prefix
     */
    private String tempFilePrefix = "rct";
    
    /**
     * async buffer size
     */
    private int asyncCacheSize = 512 * 1024;
    
    /**
     * dump rdb version
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
    
    /**
     * metric uri
     */
    private URI metricUri;
    
    /**
     * metric user
     */
    private String metricUser;
    
    /**
     * metric pass
     */
    private String metricPass;
    
    /**
     * metric gateway
     */
    private Gateway metricGateway;
    
    /**
     * metric database
     */
    private String metricDatabase;
    
    /**
     * metric retention policy
     */
    private String metricRetentionPolicy;
    
    /**
     * metric instance
     */
    private String metricInstance;

    /**
     * ssl parameter
     */
    private boolean sourceDefaultTruststore;
    
    /**
     * ssl parameter
     */
    private String sourceKeystorePath;
    
    /**
     * ssl parameter
     */
    private String sourceKeystorePass;
    
    /**
     * ssl parameter
     */
    private String sourceKeystoreType;

    /**
     * ssl parameter
     */
    private boolean targetDefaultTruststore;
    
    /**
     * ssl parameter
     */
    private String targetKeystorePath;

    /**
     * ssl parameter
     */
    private String targetKeystorePass;

    /**
     * ssl parameter
     */
    private String targetKeystoreType;
    
    /**
     * rct quote
     */
    private byte quote = '"';
    
    /**
     * rct delimiter
     */
    private byte delimiter = ',';
    
    /**
     * rct export meta
     */
    private boolean exportMeta = true;
    
    /**
     * rct export meta
     */
    private boolean exportUnit = true;
    
    /**
     * rct export format date
     */
    private boolean exportFormatDate = true;
    
    /**
     * rct export file format
     */
    private FileType exportFileFormat = FileType.CSV;
    
    /**
     * progress bar setting
     */
    private boolean enableProgressBar = true;
    
    /**
     * monitor refresh interval setting
     */
    private int monitorRefreshInterval = 15000;
    
    /**
     * enable redis scan mode
     */
    private boolean enableScan = false;
    
    /**
     * scan step
     */
    private int scanStep = 512;

    /**
     * ignore keys with ttl set
     */
    private boolean ignoreKeysWithTTL = false;

    public boolean getIgnoreKeysWithTTL() {
        return this.ignoreKeysWithTTL;
    }

    public void setIgnoreKeysWithTTL(boolean choice) {
        this.ignoreKeysWithTTL = choice;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
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

    public boolean isExportMeta() {
        return exportMeta;
    }

    public void setExportMeta(boolean exportMeta) {
        this.exportMeta = exportMeta;
    }

    public boolean isExportUnit() {
        return exportUnit;
    }

    public void setExportUnit(boolean exportUnit) {
        this.exportUnit = exportUnit;
    }
    
    public boolean isExportFormatDate() {
        return exportFormatDate;
    }
    
    public void setExportFormatDate(boolean exportFormatDate) {
        this.exportFormatDate = exportFormatDate;
    }
    
    public FileType getExportFileFormat() {
        return exportFileFormat;
    }
    
    public void setExportFileFormat(FileType exportFileFormat) {
        this.exportFileFormat = exportFileFormat;
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
    
    public int getRetries() {
        return retries;
    }
    
    public void setRetries(int retries) {
        this.retries = retries;
    }
    
    public int getMigrateBatchSize() {
        return migrateBatchSize;
    }
    
    public void setMigrateBatchSize(int migrateBatchSize) {
        this.migrateBatchSize = migrateBatchSize;
    }
    
    public int getMigrateThreads() {
        return migrateThreads;
    }
    
    public void setMigrateThreads(int migrateThreads) {
        this.migrateThreads = migrateThreads;
    }
    
    public int getMigrateRetries() {
        return migrateRetries;
    }
    
    public void setMigrateRetries(int migrateRetries) {
        this.migrateRetries = migrateRetries;
    }
    
    public boolean isMigrateFlush() {
        return migrateFlush;
    }
    
    public void setMigrateFlush(boolean migrateFlush) {
        this.migrateFlush = migrateFlush;
    }

    public int getRetryInterval() {
        return retryInterval;
    }
    
    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }
    
    public int getInputBufferSize() {
        return inputBufferSize;
    }
    
    public void setInputBufferSize(int inputBufferSize) {
        this.inputBufferSize = inputBufferSize;
    }
    
    public int getOutputBufferSize() {
        return outputBufferSize;
    }
    
    public void setOutputBufferSize(int outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }
    
    public int getMaxOutputBufferSize() {
        return maxOutputBufferSize;
    }
    
    public void setMaxOutputBufferSize(int maxOutputBufferSize) {
        this.maxOutputBufferSize = maxOutputBufferSize;
    }
    
    public String getTempFilePath() {
        return tempFilePath;
    }
    
    public void setTempFilePath(String tempFilePath) {
        this.tempFilePath = tempFilePath;
    }
    
    public String getTempFilePrefix() {
        return tempFilePrefix;
    }
    
    public void setTempFilePrefix(String tempFilePrefix) {
        this.tempFilePrefix = tempFilePrefix;
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
    
    public URI getMetricUri() {
        return metricUri;
    }
    
    public void setMetricUri(URI metricUri) {
        this.metricUri = metricUri;
    }
    
    public String getMetricUser() {
        return metricUser;
    }
    
    public void setMetricUser(String metricUser) {
        this.metricUser = metricUser;
    }
    
    public String getMetricPass() {
        return metricPass;
    }
    
    public void setMetricPass(String metricPass) {
        this.metricPass = metricPass;
    }
    
    public Gateway getMetricGateway() {
        return metricGateway;
    }
    
    public void setMetricGateway(Gateway metricGateway) {
        this.metricGateway = metricGateway;
    }

    public String getMetricDatabase() {
        return metricDatabase;
    }

    public void setMetricDatabase(String metricDatabase) {
        this.metricDatabase = metricDatabase;
    }

    public String getMetricRetentionPolicy() {
        return metricRetentionPolicy;
    }

    public void setMetricRetentionPolicy(String metricRetentionPolicy) {
        this.metricRetentionPolicy = metricRetentionPolicy;
    }

    public String getMetricInstance() {
        return metricInstance;
    }

    public void setMetricInstance(String metricInstance) {
        this.metricInstance = metricInstance;
    }

    public String getSourceKeystorePath() {
        return sourceKeystorePath;
    }

    public void setSourceKeystorePath(String sourceKeystorePath) {
        this.sourceKeystorePath = sourceKeystorePath;
    }

    public String getSourceKeystorePass() {
        return sourceKeystorePass;
    }

    public void setSourceKeystorePass(String sourceKeystorePass) {
        this.sourceKeystorePass = sourceKeystorePass;
    }

    public String getSourceKeystoreType() {
        return sourceKeystoreType;
    }

    public void setSourceKeystoreType(String sourceKeystoreType) {
        this.sourceKeystoreType = sourceKeystoreType;
    }

    public String getTargetKeystorePath() {
        return targetKeystorePath;
    }

    public void setTargetKeystorePath(String targetKeystorePath) {
        this.targetKeystorePath = targetKeystorePath;
    }

    public String getTargetKeystorePass() {
        return targetKeystorePass;
    }

    public void setTargetKeystorePass(String targetKeystorePass) {
        this.targetKeystorePass = targetKeystorePass;
    }

    public String getTargetKeystoreType() {
        return targetKeystoreType;
    }

    public void setTargetKeystoreType(String targetKeystoreType) {
        this.targetKeystoreType = targetKeystoreType;
    }

    public boolean isSourceDefaultTruststore() {
        return sourceDefaultTruststore;
    }

    public void setSourceDefaultTruststore(boolean sourceDefaultTruststore) {
        this.sourceDefaultTruststore = sourceDefaultTruststore;
    }

    public boolean isTargetDefaultTruststore() {
        return targetDefaultTruststore;
    }

    public void setTargetDefaultTruststore(boolean targetDefaultTruststore) {
        this.targetDefaultTruststore = targetDefaultTruststore;
    }
    
    public boolean isEnableProgressBar() {
        return enableProgressBar;
    }
    
    public void setEnableProgressBar(boolean enableProgressBar) {
        this.enableProgressBar = enableProgressBar;
    }
    
    public int getMonitorRefreshInterval() {
        return monitorRefreshInterval;
    }
    
    public void setMonitorRefreshInterval(int monitorRefreshInterval) {
        this.monitorRefreshInterval = monitorRefreshInterval;
    }
    
    public boolean isEnableScan() {
        return enableScan;
    }
    
    public void setEnableScan(boolean enableScan) {
        this.enableScan = enableScan;
    }
    
    public int getScanStep() {
        return scanStep;
    }
    
    public void setScanStep(int scanStep) {
        this.scanStep = scanStep;
    }
    
    public Configuration merge(RedisURI uri, boolean source) {
        Configuration base;
        if (uri != null) {
            base = Configuration.valueOf(uri);
        } else {
            base = Configuration.defaultSetting();
        }
        return merge(base, source);
    }

    public Configuration merge(RedisSentinelURI uri, boolean source) {
        return merge(valueOf(uri), source);
    }
    
    public Configuration merge(Configuration conf, boolean source) {
        conf.setRetries(this.retries);
        conf.setRetryTimeInterval(this.retryInterval);
        conf.setConnectionTimeout(this.timeout);
        conf.setReadTimeout(this.timeout);
        conf.setSendBufferSize(this.sndBuf);
        conf.setReceiveBufferSize(this.rcvBuf);
        conf.setBufferSize(this.inputBufferSize);
        conf.setAsyncCachedBytes(this.asyncCacheSize);
        conf.setVerbose(this.verbose);
        conf.setHeartbeatPeriod(this.heartbeat);
        conf.setEnableScan(this.enableScan);
        conf.setScanStep(this.scanStep);

        if (conf.isSsl()) {
            RedisSslContextFactory factory = new RedisSslContextFactory();
            if (source) {
                if (sourceKeystorePath != null) {
                    factory.setKeyStorePath(sourceKeystorePath);
                    factory.setKeyStorePassword(sourceKeystorePass);
                    factory.setKeyStoreType(sourceKeystoreType);
                    if (!sourceDefaultTruststore) {
                        factory.setTrustStorePath(sourceKeystorePath);
                        factory.setTrustStorePassword(sourceKeystorePass);
                        factory.setTrustStoreType(sourceKeystoreType);
                    }
                    
                }
            } else {
                if (targetKeystorePath != null) {
                    factory.setKeyStorePath(targetKeystorePath);
                    factory.setKeyStorePassword(targetKeystorePass);
                    factory.setKeyStoreType(targetKeystoreType);
                    if (!targetDefaultTruststore) {
                        factory.setTrustStorePath(targetKeystorePath);
                        factory.setTrustStorePassword(targetKeystorePass);
                        factory.setTrustStoreType(targetKeystoreType);
                    }
                }
            }
            conf.setSslContextFactory(factory);
        }
        return conf;
    }
    
    public static Configure bind() {
        return bind(null);
    }
    
    public static Configure bind(Properties properties) {
        Configure conf = new Configure(properties);
        conf.batchSize = getInt(conf, "batch_size", 128, true);
        conf.migrateBatchSize = getInt(conf, "migrate_batch_size", 4096, true);
        conf.migrateThreads = getInt(conf, "migrate_threads", 4, true);
        conf.migrateRetries = getInt(conf, "migrate_retries", 1, true);
        conf.migrateFlush = getBool(conf, "migrate_flush", true, true);
        conf.dumpRdbVersion = getInt(conf, "dump_rdb_version", -1, true);
        conf.retries = getInt(conf, "retries", 5, true);
        conf.retryInterval = getInt(conf, "retry_interval", 1000, true);
        conf.timeout = getInt(conf, "timeout", 60000, true);
        conf.sndBuf = getInt(conf, "snd_buf", 0, true);
        conf.rcvBuf = getInt(conf, "rcv_buf", 0, true);
        conf.inputBufferSize = getInt(conf, "input_buffer_size", 8 * 1024, true);
        conf.outputBufferSize = getInt(conf, "output_buffer_size", 8 * 1024, true);
        conf.maxOutputBufferSize = getInt(conf, "max_output_buffer_size", 512 * 1024 * 1024, true);
        conf.tempFilePath = getString(conf, "temp_file_path", null, true);
        conf.tempFilePrefix = getString(conf, "temp_file_prefix", "rct", true);
        conf.asyncCacheSize = getInt(conf, "async_cache_size", 512 * 1024, true);
        conf.verbose = getBool(conf, "verbose", false, true);
        conf.heartbeat = getInt(conf, "heartbeat", 1000, true);
        conf.metricUser = getString(conf, "metric_user", null, true);
        conf.metricPass = getString(conf, "metric_pass", null, true);
        conf.metricUri = getUri(conf, "metric_uri", "http://localhost:8086", true);
        conf.metricGateway = Gateway.parse(getString(conf, "metric_gateway", "none", true));
        conf.metricDatabase = getString(conf, "metric_database", "redis_rdb_cli", true);
        conf.metricRetentionPolicy = getString(conf, "metric_retention_policy", "30days", true);
        conf.metricInstance = getString(conf, "metric_instance", "instance0", true);
    
        conf.enableProgressBar = getBool(conf, "enable_progress_bar", true, true);
    
        conf.monitorRefreshInterval = getInt(conf, "monitor_refresh_interval", 15000, true);
        
        // scan
        conf.enableScan = getBool(conf, "enable_scan", false, true);
        conf.scanStep = getInt(conf, "scan_step", 512, true);
        
        // export
        conf.quote = (byte) getString(conf, "quote", "\"", true).charAt(0);
        conf.delimiter = (byte) getString(conf, "delimiter", ",", true).charAt(0);
        conf.exportMeta = getBool(conf, "export_meta", true, true);
        conf.exportUnit = getBool(conf, "export_unit", true, true);
        conf.exportFormatDate = getBool(conf, "export_format_date", true, true);
        conf.exportFileFormat = FileType.parse(getString(conf, "export_file_format", "csv", true));
        
        // ssl
        conf.sourceKeystorePath = getString(conf, "source_keystore_path", null, true);
        conf.sourceKeystorePass = getString(conf, "source_keystore_pass", null, true);
        conf.sourceKeystoreType = getString(conf, "source_keystore_type", "pkcs12", true);
        conf.sourceDefaultTruststore = getBool(conf, "source_default_truststore", false, true);

        conf.targetKeystorePath = getString(conf, "target_keystore_path", null, true);
        conf.targetKeystorePass = getString(conf, "target_keystore_pass", null, true);
        conf.targetKeystoreType = getString(conf, "target_keystore_type", "pkcs12", true);
        conf.targetDefaultTruststore = getBool(conf, "target_default_truststore", false, true);
        return conf;
    }
    
    public static Configuration valueOf(RedisSentinelURI uri) {
        Configuration configuration = Configuration.defaultSetting();
        Map<String, String> parameters = uri.getParameters();
        if (parameters.containsKey("connectionTimeout")) {
            configuration.setConnectionTimeout(getInt(parameters.get("connectionTimeout"), 60000));
        }
        if (parameters.containsKey("readTimeout")) {
            configuration.setReadTimeout(getInt(parameters.get("readTimeout"), 60000));
        }
        if (parameters.containsKey("receiveBufferSize")) {
            configuration.setReceiveBufferSize(getInt(parameters.get("receiveBufferSize"), 0));
        }
        if (parameters.containsKey("sendBufferSize")) {
            configuration.setSendBufferSize(getInt(parameters.get("sendBufferSize"), 0));
        }
        if (parameters.containsKey("retries")) {
            configuration.setRetries(getInt(parameters.get("retries"), 5));
        }
        if (parameters.containsKey("retryTimeInterval")) {
            configuration.setRetryTimeInterval(getInt(parameters.get("retryTimeInterval"), 1000));
        }
        if (parameters.containsKey("bufferSize")) {
            configuration.setBufferSize(getInt(parameters.get("bufferSize"), 8 * 1024));
        }
        if (parameters.containsKey("authUser")) {
            configuration.setAuthUser(parameters.get("authUser"));
        }
        if (parameters.containsKey("authPassword")) {
            configuration.setAuthPassword(parameters.get("authPassword"));
        }
        if (parameters.containsKey("discardRdbEvent")) {
            configuration.setDiscardRdbEvent(getBool(parameters.get("discardRdbEvent"), false));
        }
        if (parameters.containsKey("asyncCachedBytes")) {
            configuration.setAsyncCachedBytes(getInt(parameters.get("asyncCachedBytes"), 512 * 1024));
        }
        if (parameters.containsKey("rateLimit")) {
            configuration.setRateLimit(getInt(parameters.get("rateLimit"), 0));
        }
        if (parameters.containsKey("verbose")) {
            configuration.setVerbose(getBool(parameters.get("verbose"), false));
        }
        if (parameters.containsKey("heartbeatPeriod")) {
            configuration.setHeartbeatPeriod(getInt(parameters.get("heartbeatPeriod"), 1000));
        }
        if (parameters.containsKey("useDefaultExceptionListener")) {
            configuration.setUseDefaultExceptionListener(getBool(parameters.get("useDefaultExceptionListener"), true));
        }
        if (parameters.containsKey("ssl")) {
            configuration.setSsl(getBool(parameters.get("ssl"), false));
        }
        if (parameters.containsKey("replId")) {
            configuration.setReplId(parameters.get("replId"));
        }
        if (parameters.containsKey("replStreamDB")) {
            configuration.setReplStreamDB(getInt(parameters.get("replStreamDB"), -1));
        }
        if (parameters.containsKey("replOffset")) {
            configuration.setReplOffset(getLong(parameters.get("replOffset"), -1L));
        }
    
        // scan
        if (parameters.containsKey("enableScan")) {
            configuration.setEnableScan(getBool(parameters.get("enableScan"), false));
        }
        if (parameters.containsKey("scanStep")) {
            configuration.setScanStep(getInt(parameters.get("scanStep"), 512));
        }
        
        // redis 6
        return configuration;
    }

    public static boolean getBool(String value, boolean defaultValue) {
        if (value == null)
            return defaultValue;
        if (value.equals("false") || value.equals("no"))
            return false;
        if (value.equals("true") || value.equals("yes"))
            return true;
        return defaultValue;
    }

    public static int getInt(String value, int defaultValue) {
        if (value == null)
            return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static long getLong(String value, long defaultValue) {
        if (value == null)
            return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    public static URI getUri(Configure conf, String key) {
        return getUri(conf, key, null, false);
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

    public static List<String> getList(Configure conf, String key) {
        return getList(conf, key, null, false);
    }
    
    public static URI getUri(Configure conf, String key, String value, boolean optional) {
        String v = getString(conf, key, value, optional);
        try {
            return v == null ? null : new URI(v);
        } catch (Throwable e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
    
    public static String getString(Configure conf, String key, String value, boolean optional) {
        String v = System.getProperty(key);
        if (Strings.isEmpty(v) && Strings.isEmpty(v = conf.properties.getProperty(key)))
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

    public static List<String> getList(Configure conf, String key, String value, boolean optional) {
        String v = getString(conf, key, value == null ? null : value, optional);
        if (v == null) 
            return null;
        return Arrays.stream(v.split(",")).map(e -> e.trim()).collect(Collectors.toList());
    }
    
    @Override
    public String toString() {
        return "Configure{" +
                "batchSize=" + batchSize +
                ", migrateBatchSize=" + migrateBatchSize +
                ", migrateThreads=" + migrateThreads +
                ", migrateRetries=" + migrateRetries +
                ", migrateFlush=" + migrateFlush +
                ", timeout=" + timeout +
                ", rcvBuf=" + rcvBuf +
                ", sndBuf=" + sndBuf +
                ", retries=" + retries +
                ", retryInterval=" + retryInterval +
                ", inputBufferSize=" + inputBufferSize +
                ", outputBufferSize=" + outputBufferSize +
                ", maxOutputBufferSize=" + maxOutputBufferSize +
                ", tempFilePath='" + tempFilePath + '\'' +
                ", tempFilePrefix='" + tempFilePrefix + '\'' +
                ", asyncCacheSize=" + asyncCacheSize +
                ", dumpRdbVersion=" + dumpRdbVersion +
                ", verbose=" + verbose +
                ", heartbeat=" + heartbeat +
                ", metricUri=" + metricUri +
                ", metricUser='" + metricUser + '\'' +
                ", metricPass='" + metricPass + '\'' +
                ", metricGateway=" + metricGateway +
                ", metricDatabase='" + metricDatabase + '\'' +
                ", metricRetentionPolicy='" + metricRetentionPolicy + '\'' +
                ", metricInstance='" + metricInstance + '\'' +
                ", sourceDefaultTruststore=" + sourceDefaultTruststore +
                ", sourceKeystorePath='" + sourceKeystorePath + '\'' +
                ", sourceKeystorePass='" + sourceKeystorePass + '\'' +
                ", sourceKeystoreType='" + sourceKeystoreType + '\'' +
                ", targetDefaultTruststore=" + targetDefaultTruststore +
                ", targetKeystorePath='" + targetKeystorePath + '\'' +
                ", targetKeystorePass='" + targetKeystorePass + '\'' +
                ", targetKeystoreType='" + targetKeystoreType + '\'' +
                ", quote=" + quote +
                ", delimiter=" + delimiter +
                ", exportMeta=" + exportMeta +
                ", exportUnit=" + exportUnit +
                ", exportFormatDate=" + exportFormatDate +
                ", exportFileFormat=" + exportFileFormat +
                ", enableProgressBar=" + enableProgressBar +
                ", monitorRefreshInterval=" + monitorRefreshInterval +
                ", enableScan=" + enableScan +
                ", scanStep=" + scanStep +
                '}';
    }
}
