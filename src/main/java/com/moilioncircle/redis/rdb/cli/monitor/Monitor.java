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

import java.util.Map;

/**
 * @author Baoyi Chen
 */
public interface Monitor { 
    interface Factory {
        Monitor create(String name);
    }

    String getName();
    
    Map<MonitorKey, ? extends Counter<Long>> getLongCounters();
    
    Map<MonitorKey, ? extends Counter<Double>> getDoubleCounters();
    
    Map<MonitorKey, ? extends Gauge<Long>> getLongGauges();
    
    Map<MonitorKey, ? extends Gauge<Double>> getDoubleGauges();
    
    Map<MonitorKey, ? extends Gauge<String>> getStringGauges();

    /**
     * Counter
     */
    void add(String measurement, long count);

    void add(String measurement, long count, long time);
    
    void add(String measurement, String property, long count);
    
    void add(String measurement, String property, long count, long time);
    
    void add(String measurement, String[] properties, long count);
    
    void add(String measurement, String[] properties, long count, long time);
    
    void add(String measurement, double count);
    
    void add(String measurement, double count, long time);
    
    void add(String measurement, String property, double count);
    
    void add(String measurement, String property, double count, long time);
    
    void add(String measurement, String[] properties, double count);
    
    void add(String measurement, String[] properties, double count, long time);
    
    /**
     * Gauge
     */
    void set(String measurement, long value);
    
    void set(String measurement, double value);
    
    void set(String measurement, String value);
    
    void set(String measurement, String property, long value);
    
    void set(String measurement, String property, double value);
    
    void set(String measurement, String property, String value);
    
    void set(String measurement, String[] properties, long value);
    
    void set(String measurement, String[] properties, double value);
    
    void set(String measurement, String[] properties, String value);

}
