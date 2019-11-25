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

package com.moilioncircle.redis.rdb.cli.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Baoyi Chen
 */
public abstract class AbstractEndpoint implements Endpoint {

    protected int port;
    protected String host;
    protected List<Short> slots = new ArrayList<>();

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
    
    @Override
    public void addSlot(short slot) {
        this.slots.add(slot);
    }

    public List<Short> getSlots() {
        return this.slots;
    }

    public void setSlots(List<Short> slots) {
        this.slots = slots;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof AbstractEndpoint)) return false;
        AbstractEndpoint endpoint = (AbstractEndpoint) o;
        return port == endpoint.port && host.equals(endpoint.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(port, host);
    }
}
