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
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;

import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.collect.Iterables.all;

public class Replicas
{

    public static <C extends ReplicaCollection<? extends C>> C filterOnEndpoints(C source, Predicate<InetAddressAndPort> predicate)
    {
        return source.filter(r -> predicate.apply(r.endpoint()));
    }

    public static <C extends ReplicaCollection<? extends C>> C filterOutLocalEndpoint(C replicas)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return filterOnEndpoints(replicas, e -> !e.equals(local));
    }

    public static <C extends ReplicaCollection<? extends C>> C subtractEndpoints(C subtractFrom, Set<InetAddressAndPort> subtract)
    {
        return Replicas.filterOnEndpoints(subtractFrom, e -> !subtract.contains(e));
    }

    public static <C extends ReplicaCollection<? extends C>> C keepEndpoints(C keepIn, Set<InetAddressAndPort> keep)
    {
        return Replicas.filterOnEndpoints(keepIn, keep::contains);
    }


    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void assertFull(Replica replica)
    {
        if (!replica.isFull())
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported: " + replica);
        }
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void assertFull(Iterable<Replica> replicas)
    {
        if (!all(replicas, Replica::isFull))
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported: " + Iterables.toString(replicas));
        }
    }

    public static List<String> stringify(ReplicaCollection<?> replicas, boolean withPort)
    {
        List<String> stringEndpoints = new ArrayList<>(replicas.size());
        for (Replica replica: replicas)
        {
            stringEndpoints.add(replica.endpoint().getHostAddress(withPort));
        }
        return stringEndpoints;
    }

}
