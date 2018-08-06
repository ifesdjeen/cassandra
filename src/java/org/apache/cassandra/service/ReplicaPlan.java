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

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.ReplicaSet;
import org.apache.cassandra.service.reads.AbstractReadExecutor;

public class ReplicaPlan
{
    protected final ReplicaList allReplicas;
    // Might be modified by speculative strategy
    protected volatile ReplicaList targetReplicas;

    protected final Keyspace keyspace;
    protected final Map<InetAddressAndPort, Replica> replicaMap;

    public ReplicaPlan(Keyspace keyspace, ReplicaList allReplicas, ReplicaList targetReplicas)
    {
        this.keyspace = keyspace;
        this.allReplicas = allReplicas;
        this.targetReplicas = targetReplicas;
        this.replicaMap = Maps.newHashMapWithExpectedSize(allReplicas.size());

        for (Replica replica: allReplicas)
            replicaMap.put(replica.getEndpoint(), replica);
    }

    public Replica getReplicaFor(InetAddressAndPort endpoint)
    {
        Replica replica = replicaMap.get(endpoint);

        if (replica != null)
            return replica;

        throw new IllegalArgumentException("Cannot find replica for " + endpoint);
    }

    /**
     * Returns all of the endpoints that are replicas for the given key that were not contacted during this query.
     * If the consistency level is datacenter local, only the endpoints in the local dc will be returned.
     */
    public ReplicaCollection additionalReplicas(ConsistencyLevel consistency)
    {
        ReplicaSet contacted = new ReplicaSet(targetReplicas);

        if (consistency.isDatacenterLocal() && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
        {
            IEndpointSnitch snitch = keyspace.getReplicationStrategy().snitch;
            String localDC = DatabaseDescriptor.getLocalDataCenter();

            return allReplicas.filter(replica -> !contacted.containsReplica(replica) &&
                                                 snitch.getDatacenter(replica).equals(localDC));
        }
        else
        {
            return allReplicas.filter(replica -> !contacted.containsReplica(replica));
        }
    }

    public ReplicaList allReplicas()
    {
        return allReplicas;
    }

    public ReplicaList targetReplicas()
    {
        return targetReplicas;
    }

    public void resetTargetReplicas(ReplicaList targetReplicas)
    {
        this.targetReplicas = targetReplicas;
    }

    public ReplicaPlan withTargets(ReplicaList targets)
    {
        return new ReplicaPlan(keyspace, allReplicas, targets);
    }
}
