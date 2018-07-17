/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

public abstract class ReplicaMultimap<K, V extends ReplicaCollection>
{
    final Map<K, V> map = new HashMap<>();

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaMultimap<?, ?> that = (ReplicaMultimap<?, ?>) o;
        return Objects.equals(map, that.map);
    }

    public int hashCode()
    {
        return map.hashCode();
    }

    protected abstract V getDefault();
    protected abstract V createEmpty(K key);

    public V get(K key)
    {
        Preconditions.checkNotNull(key);
        return map.getOrDefault(key, getDefault());
    }

    public boolean put(K key, Replica replica)
    {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(replica);
        return map.computeIfAbsent(key, this::createEmpty).add(replica);
    }

    public boolean putAll(K key, ReplicaCollection replicas)
    {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(replicas);
        boolean result = false;
        for (Replica replica: replicas)
            result |= put(key, replica);
        return result;
    }

    public boolean putAll(ReplicaMultimap<K, ? extends ReplicaCollection> map)
    {
        boolean result = false;
        for (K key : map.keySet())
        {
            if (key == null)
                throw new IllegalStateException("Shouldn't be possible to have a null key");
            for (Replica replica : map.get(key))
            {
                if (replica == null)
                    throw  new IllegalStateException("Shouldn't be possible to have a null replica");
                result |= put(key, replica);
            }
        }
        return result;
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }

    public Map<K, V> asMap()
    {
        return map;
    }

    public ReplicaCollection values()
    {
        return Replicas.concat((Iterable<ReplicaCollection>) map.values());
    }

    public Iterable<Map.Entry<K, Replica>> entries()
    {
        return () -> {
            Stream<Map.Entry<K, Replica>> s = map.entrySet()
                                                 .stream()
                                                 .flatMap(entry -> entry.getValue()
                                                                        .stream()
                                                                        .map(replica -> (Map.Entry<K, Replica>)new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), replica)));
            return s.iterator();
        };
    }

    public static <K> ReplicaMultimap<K, ReplicaList> list()
    {
        return new ReplicaMultimap<K, ReplicaList>()
        {
            protected ReplicaList getDefault()
            {
                return ReplicaList.EMPTY;
            }

            protected ReplicaList createEmpty(K key)
            {
                return new ReplicaList();
            }
        };
    }

    public static <K> ReplicaMultimap<K, ReplicaSet> set()
    {
        return new ReplicaMultimap<K, ReplicaSet>()
        {
            protected ReplicaSet getDefault()
            {
                return ReplicaSet.EMPTY;
            }

            protected ReplicaSet createEmpty(K key)
            {
                return new ReplicaSet();
            }
        };
    }

    @Override
    public String toString()
    {
        return map.toString();
    }
}
