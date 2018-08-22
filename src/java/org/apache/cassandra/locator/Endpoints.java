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

import org.apache.cassandra.locator.ReplicaCollection.Mutable.Conflict;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class Endpoints<C extends Endpoints<C>> extends AbstractReplicaCollection<C>
{
    static final Map<InetAddressAndPort, Replica> EMPTY_MAP = Collections.unmodifiableMap(new LinkedHashMap<>());

    volatile Map<InetAddressAndPort, Replica> byEndpoint;

    Endpoints(List<Replica> list, boolean isSnapshot, Map<InetAddressAndPort, Replica> byEndpoint)
    {
        super(list, isSnapshot);
        this.byEndpoint = byEndpoint;
    }

    /**
     * construct a new Mutable of our own type, so that we can concatenate
     * TODO: this isn't terribly pretty, but we need sometimes to select / merge two Endpoints of unknown type;
     */
    public abstract Mutable<C> newMutable(int initialCapacity);

    @Override
    public Set<InetAddressAndPort> endpoints()
    {
        return byEndpoint().keySet();
    }

    public Map<InetAddressAndPort, Replica> byEndpoint()
    {
        Map<InetAddressAndPort, Replica> map = byEndpoint;
        if (map == null)
            byEndpoint = map = buildByEndpoint(list);
        return map;
    }

    @Override
    public boolean contains(Replica replica)
    {
        return replica != null
                && Objects.equals(
                        byEndpoint().get(replica.endpoint()),
                        replica);
    }

    private static Map<InetAddressAndPort, Replica> buildByEndpoint(List<Replica> list)
    {
        // TODO: implement a delegating map that uses our superclass' list, and is immutable
        Map<InetAddressAndPort, Replica> byEndpoint = new LinkedHashMap<>(list.size());
        for (Replica replica : list)
        {
            Replica prev = byEndpoint.put(replica.endpoint(), replica);
            assert prev == null : "duplicate endpoint in EndpointsForRange: " + prev + " and " + replica;
        }

        return Collections.unmodifiableMap(byEndpoint);
    }

    // TODO: TR-Review ignoreConflicts is not an acceptable solution here - we need to explicitly resolve them in case of transient/full mismatch
    public static <E extends Endpoints<E>> E concat(E natural, E pending, Conflict ignoreConflicts)
    {
        if (pending.isEmpty())
            return natural;
        if (natural.isEmpty())
            return pending;
        Mutable<E> mutable = natural.newMutable(natural.size() + pending.size());
        mutable.addAll(natural);
        mutable.addAll(pending, ignoreConflicts);
        return mutable.asImmutableView();
    }

}
