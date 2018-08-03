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

package com.moilioncircle.redis.rdb.cli.metric;

import io.dropwizard.metrics5.MetricName;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Baoyi Chen
 */
public class MetricNames {
    public static MetricName name(String name, String... tags) {
        if ((tags.length & 1) != 0) {
            throw new IllegalArgumentException("tag.length " + tags.length + " illegal");
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < tags.length; i += 2) {
            map.put(tags[i], tags[i + 1]);
        }
        return new MetricName(name, map);
    }
}
