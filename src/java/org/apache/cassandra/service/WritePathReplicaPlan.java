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

package org.apache.cassandra.service;

import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;

public class WritePathReplicaPlan extends ReplicaPlan
{
    private final Endpoints<?> pendingReplicas;

    public WritePathReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, Endpoints<?> allReplicas, Endpoints<?> targetReplicas, Endpoints<?> pendingReplicas)
    {
        super(keyspace, consistencyLevel, allReplicas, targetReplicas);
        this.pendingReplicas = pendingReplicas;
    }

    public Endpoints<?> pendingReplicas()
    {
        return pendingReplicas;
    }

    public static <E extends Endpoints<E>> WritePathReplicaPlan createReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, E naturalReplicas, E pendingReplicas) throws UnavailableException
    {
        E replicas = Endpoints.concat(naturalReplicas, pendingReplicas, true);
        return new WritePathReplicaPlan(keyspace, consistencyLevel, replicas, replicas, pendingReplicas);
    }

    /**
     * We want to send mutations to as many full replicas as we can, and just as many transient replicas
     * as we need to meet blockFor.
     */
    public static <E extends Endpoints<E>> WritePathReplicaPlan createReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, E naturalReplicas, E pendingReplicas, Predicate<InetAddressAndPort> livePredicate) throws UnavailableException
    {
        if (!keyspace.getReplicationStrategy().hasTransientReplicas())
        {
            E replicas = Endpoints.concat(naturalReplicas, pendingReplicas, true);
            return new WritePathReplicaPlan(keyspace, consistencyLevel, replicas, replicas, pendingReplicas);
        }

        return createReplicaPlan(keyspace, consistencyLevel, consistencyLevel.blockFor(keyspace), naturalReplicas, pendingReplicas, livePredicate);
    }

    @VisibleForTesting
    static <E extends Endpoints<E>> WritePathReplicaPlan createReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, int blockFor, E naturalReplicas, E pendingReplicas, Predicate<InetAddressAndPort> livePredicate) throws UnavailableException
    {
        // TODO: TR-REVIEW why are we filtering to only isFull here? surely if any transient range is 'pending' it should be receiving writes too?
        pendingReplicas = pendingReplicas.filter(Replica::isFull);
        E allReplicas = Endpoints.concat(naturalReplicas, pendingReplicas, true);
        E selectedReplicas = allReplicas.select()
                .add(r -> r.isFull() && livePredicate.test(r.endpoint()))
                .add(r -> r.isTransient() && livePredicate.test(r.endpoint()), blockFor)
                .get();

        if (selectedReplicas.size() < blockFor)
            throw new UnavailableException(consistencyLevel, blockFor, selectedReplicas.size());

        if (selectedReplicas.isEmpty() || selectedReplicas.get(0).isTransient())
            throw new UnavailableException("At least one full replica required for writes", consistencyLevel, blockFor, 0);

        return new WritePathReplicaPlan(keyspace, consistencyLevel, allReplicas, selectedReplicas, pendingReplicas);
    }
}
