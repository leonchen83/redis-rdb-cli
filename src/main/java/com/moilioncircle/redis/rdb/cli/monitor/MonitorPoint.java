/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.rdb.cli.monitor;

import static com.moilioncircle.redis.rdb.cli.glossary.MonitorType.COUNTER;
import static com.moilioncircle.redis.rdb.cli.glossary.MonitorType.GAUGE;

import com.moilioncircle.redis.rdb.cli.glossary.MonitorType;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Counter;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Gauge;
import com.moilioncircle.redis.rdb.cli.monitor.entity.Monitor;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Jingqi Xu
 */
public final class MonitorPoint {
    //
    private long time;
    private long value;
    private long timestamp;
    private String monitorKey;
    private String monitorName;
    private MonitorType monitorType;

    /**
     *
     */
    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMonitorKey() {
        return monitorKey;
    }

    public void setMonitorKey(String key) {
        this.monitorKey = key;
    }

    public String getMonitorName() {
        return monitorName;
    }

    public void setMonitorName(String name) {
        this.monitorName = name;
    }

    public MonitorType getMonitorType() {
        return monitorType;
    }

    public void setMonitorType(MonitorType type) {
        this.monitorType = type;
    }

    public static final MonitorPoint valueOf(Monitor m, String k, Gauge v) {
        long now = System.currentTimeMillis();
        return valueOf(now, m, k, GAUGE, null, v.getGauge(), 0L);
    }

    public static final MonitorPoint valueOf(Monitor m, String k, Counter v) {
        long now = System.currentTimeMillis();
        Tuple2<Long, Long> p = v.getCounter();
        return valueOf(now, m, k, COUNTER, null, p.getV1(), p.getV2());
    }

    protected static MonitorPoint valueOf(long now, Monitor m, String k, MonitorType t, String p, long value, long time) {
        final MonitorPoint r = new MonitorPoint();
        r.monitorName = m.getName();
        r.monitorType = t;
        r.monitorKey = k;
        r.timestamp = now;
        r.time = time;
        r.value = value;
        return r;
    }

    @Override
    public String toString() {
        return "MonitorPoint{" +
                "time=" + time +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", monitorKey='" + monitorKey + '\'' +
                ", monitorName='" + monitorName + '\'' +
                ", monitorType=" + monitorType +
                '}';
    }
}
