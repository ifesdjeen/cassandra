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

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.Replicas;

public class WritePathReplicaPlan extends ReplicaPlan
{
    private final ReplicaCollection pendingReplicas;

    public WritePathReplicaPlan(Keyspace keyspace, ReplicaList allReplicas, ReplicaList targetReplicas, ReplicaCollection pendingReplicas)
    {
        super(keyspace, allReplicas, targetReplicas);
        this.pendingReplicas = pendingReplicas;
    }

    public ReplicaCollection pendingReplicas()
    {
        return pendingReplicas;
    }

    public static WritePathReplicaPlan createReplicaPlan(Keyspace keyspace, ReplicaList naturalReplicas, ReplicaCollection pendingReplicas) throws UnavailableException
    {
        ReplicaList replicas = new ReplicaList(Replicas.concatNaturalAndPending(naturalReplicas, pendingReplicas));
        return new WritePathReplicaPlan(keyspace, replicas, replicas, pendingReplicas);
    }

    /**
     * We want to send mutations to as many full replicas as we can, and just as many transient replicas
     * as we need to meet blockFor.
     */
    public static WritePathReplicaPlan createReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaList naturalReplicas, ReplicaCollection pendingReplicas, Predicate<InetAddressAndPort> livePredicate) throws UnavailableException
    {
        if (!keyspace.getReplicationStrategy().hasTransientReplicas())
        {
            ReplicaList replicas = new ReplicaList(Replicas.concatNaturalAndPending(naturalReplicas, pendingReplicas));
            return new WritePathReplicaPlan(keyspace, replicas, replicas, pendingReplicas);
        }

        return createReplicaPlan(keyspace, consistencyLevel, consistencyLevel.blockFor(keyspace), naturalReplicas, pendingReplicas, livePredicate);
    }

    @VisibleForTesting
    static WritePathReplicaPlan createReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, int blockFor, ReplicaList naturalReplicas, ReplicaCollection pendingReplicas, Predicate<InetAddressAndPort> livePredicate) throws UnavailableException
    {
        pendingReplicas = new ReplicaList(Replicas.filter(pendingReplicas, Replica::isFull));
        ReplicaList replicas = new ReplicaList(Replicas.concatNaturalAndPending(naturalReplicas, pendingReplicas));

        ReplicaList initial = new ReplicaList(replicas.size());
        Queue<Replica> transients = new LinkedList<>();
        int liveRecipients = 0;
        for (Replica replica : replicas)
        {
            if (replica.isFull())
            {
                if (livePredicate.test(replica.getEndpoint()))
                {
                    initial.add(replica);
                    liveRecipients++;
                }
            }
            else
            {
                transients.offer(replica);
            }
        }

        if (liveRecipients == 0)
            throw new UnavailableException("At least one full replica required for writes", consistencyLevel, blockFor, 0);

        // add transient replicas to the initial recipients until we've met blockFor
        while (liveRecipients < blockFor)
        {
            if (transients.isEmpty())
                throw new UnavailableException(consistencyLevel, blockFor, liveRecipients);

            Replica replica = transients.remove();
            if (livePredicate.test(replica.getEndpoint()))
            {
                initial.add(replica);
                liveRecipients++;
            }
        }

        return new WritePathReplicaPlan(keyspace, replicas, initial, pendingReplicas);
    }
}
