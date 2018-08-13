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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;

public class ReplicaPlan
{
    // TODO: ideally these would be generified to either EndpointsByToken or EndpointsByRange, but currently we conflate
    // the two in a couple of places (primarily Digest/DataResolver, which can be for a range or partition read)
    protected final Endpoints<?> allReplicas;
    // Might be modified by speculative strategy
    protected volatile Endpoints<?> targetReplicas;

    protected final Keyspace keyspace;
    protected final ConsistencyLevel consistencyLevel;
    protected final Map<InetAddressAndPort, Replica> replicaMap;

    public ReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, Endpoints<?> allReplicas, Endpoints<?> targetReplicas)
    {
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.allReplicas = allReplicas;
        this.targetReplicas = targetReplicas;
        this.replicaMap = allReplicas.byEndpoint();
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
    public Endpoints<?> additionalReplicas()
    {
        if (consistencyLevel.isDatacenterLocal() && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
        {
            IEndpointSnitch snitch = keyspace.getReplicationStrategy().snitch;
            String localDC = DatabaseDescriptor.getLocalDataCenter();

            return allReplicas.filter(replica -> !targetReplicas.contains(replica) &&
                                                 snitch.getDatacenter(replica).equals(localDC));
        }
        else
        {
            return allReplicas.filter(replica -> !targetReplicas.contains(replica));
        }
    }

    public Endpoints<?> allReplicas()
    {
        return allReplicas;
    }

    public Endpoints<?> targetReplicas()
    {
        return targetReplicas;
    }

    public void resetTargetReplicas(Endpoints<?> targetReplicas)
    {
        this.targetReplicas = targetReplicas;
    }

    public Keyspace keyspace()
    {
        return keyspace;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistencyLevel;
    }

    public ReplicaPlan with(Endpoints<?> targetReplicas)
    {
        return new ReplicaPlan(keyspace, consistencyLevel, allReplicas, targetReplicas);
    }

    public ReplicaPlan with(ConsistencyLevel consistencyLevel)
    {
        return new ReplicaPlan(keyspace, consistencyLevel, allReplicas, targetReplicas);
    }
}

