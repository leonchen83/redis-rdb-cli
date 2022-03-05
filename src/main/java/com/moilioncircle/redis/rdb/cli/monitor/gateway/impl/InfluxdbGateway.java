package com.moilioncircle.redis.rdb.cli.monitor.gateway.impl;

import static com.moilioncircle.redis.rdb.cli.util.Collections.isEmpty;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.influxdb.BatchOptions.DEFAULTS;
import static org.influxdb.InfluxDB.ConsistencyLevel.ONE;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.monitor.gateway.MetricGateway;
import com.moilioncircle.redis.rdb.cli.monitor.points.CounterPoint;
import com.moilioncircle.redis.rdb.cli.monitor.points.GaugePoint;
import com.moilioncircle.redis.rdb.cli.util.XThreadFactory;

import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

/**
 * @author Baoyi Chen
 */
public class InfluxdbGateway implements MetricGateway {
    private static final Logger logger = LoggerFactory.getLogger(InfluxdbGateway.class);

    public static final String VALUE = "value";
    public static final String MTIME = "mtime";
    public static final String PROPERTY = "property";
    public static final String INSTANCE = "instance";

    protected int port;
    protected int threads = 1;
    protected String instance;
    protected InfluxDB influxdb;
    protected int actions = 256;
    protected int jitter = 1000;
    protected int interval = 2000;
    protected int capacity = 8192;
    protected Configure configure;
    protected String retention = "autogen";
    protected ConsistencyLevel consistency = ONE;
    protected String url, database, user, password;

    public InfluxdbGateway(Configure configure) {
        this.configure = configure;
        this.user = configure.getMetricUser();
        this.password = configure.getMetricPass();
        this.instance = configure.getMetricInstance();
        this.url = configure.getMetricUri().toString();
        this.database = configure.getMetricDatabase();
        this.retention = configure.getMetricRetentionPolicy();
        this.influxdb = create();
    }
    
    @Override
    public void reset(String measurement) {
        if (this.influxdb != null) {
            try {
                this.influxdb.query(new Query("drop series from \"" + measurement + "\" where instance = '" + instance + "'", database));
            } catch (Throwable e) {
                logger.error("failed to reset measurement [{}]. cause {}", measurement, e.getMessage());
                if (e instanceof ConnectException) {
                    System.err.println("failed to reset measurement [" + measurement + "]. cause " + e.getMessage());
                    System.err.println("run `cd /path/to/tair-cli/dashboard & docker-compose up -d` to start dashboard");
                    System.exit(-1);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.influxdb != null) this.influxdb.close();
    }

    @Override
    public boolean save(List<GaugePoint<?>> gauges, List<CounterPoint<?>> counters) {
        //
        if (isEmpty(gauges) && isEmpty(counters)) {
            return false;
        }

        //
        try {
            for (Point p : toGaugePoints(gauges)) influxdb.write(p);
            for (Point p : toCounterPoints(counters)) influxdb.write(p);
            return true;
        } catch (Throwable t) {
            logger.error("failed to save points. cause {}", t.getMessage());
            return false;
        }
    }

    protected List<Point> toCounterPoints(List<CounterPoint<?>> counters) {
        final List<Point> r = new ArrayList<>((counters.size()));
        for (CounterPoint<?> point : counters) r.add(toCounterPoint(point));
        return r;
    }

    protected Point toCounterPoint(CounterPoint<?> point) {
        final String name = point.getMonitorName();
        Point.Builder builder = Point.measurement(name);
        builder.time(point.getTimestamp(), MILLISECONDS);
        if (point.getValue() instanceof Long) {
            builder.addField(VALUE, (Long) point.getValue());
        } else if (point.getValue() instanceof Double) {
            builder.addField(VALUE, (Double) point.getValue());
        } else if (point.getValue() instanceof String) {
            builder.addField(VALUE, (String) point.getValue());
        }
        builder.tag(INSTANCE, instance);
        if (point.getTime() > 0) builder.addField(MTIME, point.getTime());
        if (point.getProperty() != null) builder.tag(PROPERTY, point.getProperty());
        return builder.build();
    }
    
    protected List<Point> toGaugePoints(List<GaugePoint<?>> points) {
        final List<Point> r = new ArrayList<>((points.size()));
        for (GaugePoint<?> point : points) r.add(toGaugePoint(point));
        return r;
    }
    
    protected Point toGaugePoint(GaugePoint<?> point) {
        final String name = point.getMonitorName();
        Point.Builder builder = Point.measurement(name);
        builder.time(point.getTimestamp(), MILLISECONDS);
        if (point.getValue() instanceof Long) {
            builder.addField(VALUE, (Long) point.getValue());
        } else if (point.getValue() instanceof Double) {
            builder.addField(VALUE, (Double) point.getValue());
        } else if (point.getValue() instanceof String) {
            builder.addField(VALUE, (String) point.getValue());
        }
        builder.tag(INSTANCE, instance);
        if (point.getProperty() != null) builder.tag(PROPERTY, point.getProperty());
        return builder.build();
    }
    
    public class ExceptionHandler implements BiConsumer<Iterable<Point>, Throwable> {
        @Override
        public void accept(final Iterable<Point> points, final Throwable t) {
            logger.warn("failed to save points. cause {}", t.getMessage());
        }
    }

    protected InfluxDB create() {
        //
        final OkHttpClient.Builder http = new OkHttpClient.Builder();
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(2); dispatcher.setMaxRequestsPerHost(2);
        http.dispatcher(dispatcher);
        http.connectionPool(new ConnectionPool(threads, 5, TimeUnit.MINUTES));
        
        //
        final InfluxDB r = InfluxDBFactory.connect(url, user, password, http);
        BatchOptions opt = DEFAULTS;
        opt = opt.consistency(consistency).jitterDuration(jitter);
        opt = opt.actions(this.actions).threadFactory(new XThreadFactory("influxdb"));
        opt = opt.exceptionHandler(new ExceptionHandler()).bufferLimit(capacity).flushDuration(interval);
        r.setDatabase(this.database).setRetentionPolicy(retention).enableBatch((opt)).enableGzip();
        return r;
    }
}
