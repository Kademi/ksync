/*
 * Copyright 2012 McEvoy Software Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.milton.sync;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.hashsplit4j.api.HashCache;

/**
 * An LRU cache of hashes.
 *
 * Was ConcurrentLinkedHashMap, which has been unmaintained since 2013 and reaches for
 * sun.misc.Unsafe, so every run on a current jvm printed a deprecation warning. Guava's cache
 * gives the same bounded LRU and is already a dependency.
 *
 * @author brad
 */
public class MemoryHashCache implements HashCache {

    private final Cache<String, String> cache;

    public MemoryHashCache() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(5000)
                .initialCapacity(2000)
                .build();
    }

    @Override
    public boolean hasHash(String key) {
        return cache.getIfPresent(key) != null;
    }

    @Override
    public void setHash(String key) {
        cache.put(key, key);
    }
}
