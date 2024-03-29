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

package com.moilioncircle.redis.rdb.cli.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * @author Baoyi Chen
 */
@SuppressWarnings("unchecked")
public class Iterators {

    public static Iterator<?> EMPTY = new Iterator<Object>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object next() {
            return null;
        }
    };

    @SafeVarargs
    public static <T> Iterator<T> iterator(final T... t) {
        class Iter implements Iterator<T> {
            private int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < t.length;
            }

            @Override
            public T next() {
                if (!hasNext()) throw new NoSuchElementException();
                return t[idx++];
            }
        }
        return t == null ? (Iterator<T>) EMPTY : new Iter();
    }
    
    public static <T> T find(Iterator<T> it, Predicate<T> condition) {
        if (it == null) return null;
        while (it.hasNext()) {
            T t = it.next();
            if (condition.test(t)) {
                return t;
            }
        }
        return null;
    }
}
