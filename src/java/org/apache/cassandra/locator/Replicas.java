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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.utils.FBUtilities;

public class Replicas
{
    private static abstract class ImmutableReplicaContainer extends ReplicaCollection
    {
        @Override
        public boolean add(Replica replica)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addAll(Iterable<Replica> replicas)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeEndpoint(InetAddressAndPort endpoint)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeReplica(Replica replica)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeReplicas(ReplicaCollection replicas)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static ReplicaCollection filter(ReplicaCollection source, Predicate<Replica> predicate)
    {
        Iterable<Replica> iterable = Iterables.filter(source, predicate);
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return Iterables.size(iterable);
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }

            @Override
            protected Collection<Replica> getUnmodifiableCollection()
            {
                return ImmutableList.copyOf(iterable);
            }

            @Override
            public Stream<Replica> stream()
            {
                return StreamSupport.stream(iterable.spliterator(), false);
            }
        };
    }

    public static ReplicaCollection filterOnEndpoints(ReplicaCollection source, Predicate<InetAddressAndPort> predicate)
    {
        Preconditions.checkNotNull(predicate);
        Iterable<Replica> iterable = Iterables.filter(source, r -> predicate.apply(r.getEndpoint()));
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return Iterables.size(iterable);
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }

            @Override
            protected Collection<Replica> getUnmodifiableCollection()
            {
                return ImmutableList.copyOf(iterable);
            }

            @Override
            public Stream<Replica> stream()
            {
                return StreamSupport.stream(iterable.spliterator(), false);
            }
        };
    }

    public static ReplicaCollection filterOutLocalEndpoint(ReplicaCollection replicas)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return filterOnEndpoints(replicas, e -> !e.equals(local));
    }

    public static ReplicaCollection concatNaturalAndPending(ReplicaCollection natural, ReplicaCollection pending)
    {
        Preconditions.checkNotNull(natural);
        Preconditions.checkNotNull(pending);
        Iterable<Replica> iterable;
        if (Iterables.all(natural, Replica::isFull) && Iterables.all(pending, Replica::isFull))
        {
            iterable = Iterables.concat(natural, pending);
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }

        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return natural.size() + pending.size();
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }

            @Override
            protected Collection<Replica> getUnmodifiableCollection()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Stream<Replica> stream()
            {
                return StreamSupport.stream(iterable.spliterator(), false);
            }
        };
    }

    public static ReplicaCollection concat(Iterable<ReplicaCollection> replicasIterable)
    {
        Preconditions.checkNotNull(replicasIterable);
        Iterable<Replica> iterable = Iterables.concat(replicasIterable);
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return Iterables.size(iterable);
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }

            @Override
            protected Collection<Replica> getUnmodifiableCollection()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Stream<Replica> stream()
            {
                return StreamSupport.stream(iterable.spliterator(), false);
            }
        };
    }

    public static ReplicaCollection of(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return 1;
            }

            protected Collection<Replica> getUnmodifiableCollection()
            {
                return Collections.singleton(replica);
            }

            public Iterator<Replica> iterator()
            {
                return Iterators.singletonIterator(replica);
            }

            @Override
            public Stream<Replica> stream()
            {
                return Stream.of(replica);
            }
        };
    }

    public static ReplicaCollection of(Collection<Replica> replicas)
    {
        Preconditions.checkNotNull(replicas);
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return replicas.size();
            }

            public Iterator<Replica> iterator()
            {
                return replicas.iterator();
            }

            @Override
            protected Collection<Replica> getUnmodifiableCollection()
            {
                return Collections.unmodifiableCollection(replicas);
            }

            @Override
            public Stream<Replica> stream()
            {
                return StreamSupport.stream(replicas.spliterator(), false);
            }
        };
    }

    private static ReplicaCollection EMPTY = new ImmutableReplicaContainer()
    {
        public int size()
        {
            return 0;
        }

        protected Collection<Replica> getUnmodifiableCollection()
        {
            return Collections.emptyList();
        }

        public Iterator<Replica> iterator()
        {
            return Collections.emptyIterator();
        }

        @Override
        public Stream<Replica> stream()
        {
            return Stream.empty();
        }
    };

    public static ReplicaCollection empty()
    {
        return EMPTY;
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Replica replica)
    {
        if (!replica.isFull())
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Iterable<Replica> replicas)
    {
        if (!Iterables.all(replicas, Replica::isFull))
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static List<String> stringify(ReplicaCollection replicas, boolean withPort)
    {
        List<String> stringEndpoints = new ArrayList<>(replicas.size());
        for (Replica replica: replicas)
        {
            stringEndpoints.add(replica.getEndpoint().getHostAddress(withPort));
        }
        return stringEndpoints;
    }
}
