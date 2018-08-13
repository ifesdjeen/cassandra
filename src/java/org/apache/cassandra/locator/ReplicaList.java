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

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ReplicaList extends AbstractReplicaCollection<ReplicaList>
{
    private static final ReplicaList EMPTY = new ReplicaList(Collections.emptyList(), true);

    private volatile Set<InetAddressAndPort> endpoints;
    private ReplicaList(List<Replica> list, boolean isSnapshot)
    {
        super(list, isSnapshot);
    }

    @Override
    public Set<InetAddressAndPort> endpoints()
    {
        Set<InetAddressAndPort> result = endpoints;
        if (result == null)
            endpoints = result = Collections.unmodifiableSet(new LinkedHashSet<>(Collections2.transform(list, Replica::endpoint)));
        return result;
    }

    @Override
    protected ReplicaList snapshot(List<Replica> subList)
    {
        return subList.isEmpty()
                ? empty()
                : new ReplicaList(subList, true);
    }

    @Override
    public ReplicaList self()
    {
        return this;
    }

    @Override
    public boolean contains(Replica replica)
    {
        return list.contains(replica);
    }

    public static class Mutable extends ReplicaList implements ReplicaCollection.Mutable<ReplicaList>
    {
        boolean hasSnapshot;
        public Mutable() { this(0); }
        public Mutable(int capacity) { super(new ArrayList<>(capacity), false); }

        public void add(Replica replica, boolean ignoreConflict)
        {
            if (hasSnapshot) throw new IllegalStateException();
            Preconditions.checkNotNull(replica);
            list.add(replica);
            super.endpoints = null;
        }

        public ReplicaList asImmutableView()
        {
            return new ReplicaList(super.list, false);
        }

        public ReplicaList asSnapshot()
        {
            hasSnapshot = true;
            return new ReplicaList(super.list, true);
        }
    }

    public static class Builder extends ReplicaCollection.Builder<ReplicaList, Mutable, ReplicaList.Builder>
    {
        public Builder() { this(0); }
        public Builder(int capacity) { super (new Mutable(capacity)); }
    }

    public static Builder builder()
    {
        return new Builder();
    }
    public static Builder builder(int capacity)
    {
        return new Builder(capacity);
    }

    public static ReplicaList empty()
    {
        return EMPTY;
    }

    public static ReplicaList of(Replica replica)
    {
        ArrayList<Replica> one = new ArrayList<>(1);
        one.add(replica);
        return new ReplicaList(one, true);
    }

    public static ReplicaList of(Replica ... replicas)
    {
        return copyOf(Arrays.asList(replicas));
    }

    public static ReplicaList copyOf(Collection<Replica> replicas)
    {
        return builder(replicas.size()).addAll(replicas).build();
    }
}
