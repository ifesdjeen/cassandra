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
package org.apache.cassandra.db;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.transport.ProtocolException;

public enum ConsistencyLevel
{
    ANY         (0),
    ONE         (1),
    TWO         (2),
    THREE       (3),
    QUORUM      (4),
    ALL         (5),
    LOCAL_QUORUM(6, true),
    EACH_QUORUM (7),
    SERIAL      (8),
    LOCAL_SERIAL(9),
    LOCAL_ONE   (10, true),
    NODELOCAL   (11, true);

    private static final Logger logger = LoggerFactory.getLogger(ConsistencyLevel.class);

    // Used by the binary protocol
    public final int code;
    private final boolean isDCLocal;
    private static final ConsistencyLevel[] codeIdx;
    static
    {
        int maxCode = -1;
        for (ConsistencyLevel cl : ConsistencyLevel.values())
            maxCode = Math.max(maxCode, cl.code);
        codeIdx = new ConsistencyLevel[maxCode + 1];
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            if (codeIdx[cl.code] != null)
                throw new IllegalStateException("Duplicate code");
            codeIdx[cl.code] = cl;
        }
    }

    private ConsistencyLevel(int code)
    {
        this(code, false);
    }

    private ConsistencyLevel(int code, boolean isDCLocal)
    {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    public static ConsistencyLevel fromCode(int code)
    {
        if (code < 0 || code >= codeIdx.length)
            throw new ProtocolException(String.format("Unknown code %d for a consistency level", code));
        return codeIdx[code];
    }

    private int quorumFor(Keyspace keyspace)
    {
        return (keyspace.getReplicationStrategy().getReplicationFactor().replicas / 2) + 1;
    }

    private int localQuorumFor(Keyspace keyspace, String dc)
    {
        return (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
             ? (((NetworkTopologyStrategy) keyspace.getReplicationStrategy()).getReplicationFactor(dc).replicas / 2) + 1
             : quorumFor(keyspace);
    }

    public int blockFor(Keyspace keyspace)
    {
        switch (this)
        {
            case ONE:
            case LOCAL_ONE:
                return 1;
            case ANY:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case SERIAL:
                return quorumFor(keyspace);
            case ALL:
                return keyspace.getReplicationStrategy().getReplicationFactor().replicas;
            case LOCAL_QUORUM:
            case LOCAL_SERIAL:
                return localQuorumFor(keyspace, DatabaseDescriptor.getLocalDataCenter());
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
                    int n = 0;
                    for (String dc : strategy.getDatacenters())
                        n += localQuorumFor(keyspace, dc);
                    return n;
                }
                else
                {
                    return quorumFor(keyspace);
                }
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
        }
    }

    public int blockForWrite(Keyspace keyspace, Endpoints<?> pending)
    {
        assert pending != null;

        int blockFor = blockFor(keyspace);
        switch (this)
        {
            case ANY:
                break;
            case LOCAL_ONE: case LOCAL_QUORUM: case LOCAL_SERIAL:
                // we will only count local replicas towards our response count, as these queries only care about local guarantees
                blockFor += countLocalEndpoints(pending);
                break;
            case ONE: case TWO: case THREE:
            case QUORUM: case EACH_QUORUM:
            case SERIAL:
            case ALL:
                blockFor += pending.size();
        }
        return blockFor;
    }

    /**
     * Determine if this consistency level meets or exceeds the consistency requirements of the given cl for the given keyspace
     */
    public boolean satisfies(ConsistencyLevel other, Keyspace keyspace)
    {
        return blockFor(keyspace) >= other.blockFor(keyspace);
    }

    public boolean isDatacenterLocal()
    {
        return isDCLocal;
    }

    public static boolean isLocal(InetAddressAndPort endpoint)
    {
        return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint));
    }

    public boolean isLocal(Replica replica)
    {
        return isLocal(replica.endpoint());
    }

    public int countLocalEndpoints(ReplicaCollection<?> liveReplicas)
    {
        int count = 0;
        for (Replica replica : liveReplicas)
            if (isLocal(replica))
                count++;
        return count;
    }

    private Map<String, Integer> countPerDCEndpoints(Keyspace keyspace, Iterable<Replica> liveReplicas)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        Map<String, Integer> dcEndpoints = new HashMap<String, Integer>();
        for (String dc: strategy.getDatacenters())
            dcEndpoints.put(dc, 0);

        for (Replica replica : liveReplicas)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(replica);
            dcEndpoints.put(dc, dcEndpoints.get(dc) + 1);
        }
        return dcEndpoints;
    }

    public <E extends Endpoints<E>> E filterForQuery(Keyspace keyspace, E liveReplicas)
    {
        return filterForQuery(keyspace, liveReplicas, false);
    }

    public <E extends Endpoints<E>> E filterForQuery(Keyspace keyspace, E liveReplicas, boolean alwaysSpeculate)
    {
        /*
         * If we are doing an each quorum query, we have to make sure that the endpoints we select
         * provide a quorum for each data center. If we are not using a NetworkTopologyStrategy,
         * we should fall through and grab a quorum in the replication strategy.
         *
         * We do not speculate for EACH_QUORUM.
         */
        if (this == EACH_QUORUM && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
            return filterForEachQuorum(keyspace, liveReplicas);

        /*
         * Endpoints are expected to be restricted to live replicas, sorted by snitch preference.
         * For LOCAL_QUORUM, move local-DC replicas in front first as we need them there whether
         * we do read repair (since the first replica gets the data read) or not (since we'll take
         * the blockFor first ones).
         */
        if (isDCLocal)
            liveReplicas = liveReplicas.sorted(DatabaseDescriptor.getLocalComparator());

        return liveReplicas.subList(0, Math.min(liveReplicas.size(), blockFor(keyspace) + (alwaysSpeculate ? 1 : 0)));
    }

    private <E extends Endpoints<E>> E filterForEachQuorum(Keyspace keyspace, E liveReplicas)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
        Map<String, Integer> dcsReplicas = new HashMap<>();
        for (String dc : strategy.getDatacenters())
        {
            // we put _up to_ dc replicas only
            dcsReplicas.put(dc, localQuorumFor(keyspace, dc));
        }

        return liveReplicas.filter((replica) -> {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(replica);
            int replicas = dcsReplicas.get(dc);
            if (replicas > 0)
            {
                dcsReplicas.put(dc, --replicas);
                return true;
            }
            return false;
        });
    }

    public boolean isSufficientLiveNodes(Keyspace keyspace, Endpoints<?> liveReplicas)
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_ONE:
                return countLocalEndpoints(liveReplicas) >= 1;
            case LOCAL_QUORUM:
                return countLocalEndpoints(liveReplicas) >= blockFor(keyspace);
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    for (Map.Entry<String, Integer> entry : countPerDCEndpoints(keyspace, liveReplicas).entrySet())
                    {
                        if (entry.getValue() < localQuorumFor(keyspace, entry.getKey()))
                            return false;
                    }
                    return true;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                return liveReplicas.size() >= blockFor(keyspace);
        }
    }

    public void assureSufficientLiveNodesForRead(Keyspace keyspace, Endpoints<?> liveReplicas) throws UnavailableException
    {
        assureSufficientLiveNodes(keyspace, liveReplicas, blockFor(keyspace));
    }
    public void assureSufficientLiveNodesForWrite(Keyspace keyspace, Endpoints<?> liveNatural, Endpoints<?> pendingWithDown) throws UnavailableException
    {
        assureSufficientLiveNodes(keyspace, liveNatural, blockForWrite(keyspace, pendingWithDown));
    }
    public void assureSufficientLiveNodes(Keyspace keyspace, Endpoints<?> liveNatural, int blockFor) throws UnavailableException
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                break;
            case LOCAL_ONE:
                if (countLocalEndpoints(liveNatural) == 0)
                    throw new UnavailableException(this, 1, 0);
                break;
            case LOCAL_QUORUM:
                int localLive = countLocalEndpoints(liveNatural);
                if (localLive < blockFor)
                {
                    if (logger.isTraceEnabled())
                    {
                        StringBuilder builder = new StringBuilder("Local replicas [");
                        for (Replica replica : liveNatural)
                        {
                            if (isLocal(replica))
                                builder.append(replica).append(',');
                        }
                        builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockFor).append(" live nodes in '").append(DatabaseDescriptor.getLocalDataCenter()).append("'");
                        logger.trace(builder.toString());
                    }
                    throw new UnavailableException(this, blockFor, localLive);
                }
                break;
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    for (Map.Entry<String, Integer> entry : countPerDCEndpoints(keyspace, liveNatural).entrySet())
                    {
                        int dcBlockFor = localQuorumFor(keyspace, entry.getKey());
                        int dcLive = entry.getValue();
                        if (dcLive < dcBlockFor)
                            throw new UnavailableException(this, entry.getKey(), dcBlockFor, dcLive);
                    }
                    break;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                int live = liveNatural.size();
                if (live < blockFor)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(liveNatural), blockFor);
                    throw new UnavailableException(this, blockFor, live);
                }
                break;
        }
    }

    public void validateForRead(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case ANY:
                throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
        }
    }

    public void validateForWrite(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException("You must use conditional updates for serializable writes");
        }
    }

    // This is the same than validateForWrite really, but we include a slightly different error message for SERIAL/LOCAL_SERIAL
    public void validateForCasCommit(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case EACH_QUORUM:
                requireNetworkTopologyStrategy(keyspaceName);
                break;
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException(this + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
        }
    }

    public void validateForCas() throws InvalidRequestException
    {
        if (!isSerialConsistency())
            throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
    }

    public boolean isSerialConsistency()
    {
        return this == SERIAL || this == LOCAL_SERIAL;
    }

    public void validateCounterForWrite(TableMetadata metadata) throws InvalidRequestException
    {
        if (this == ConsistencyLevel.ANY)
            throw new InvalidRequestException("Consistency level ANY is not yet supported for counter table " + metadata.name);

        if (isSerialConsistency())
            throw new InvalidRequestException("Counter operations are inherently non-serializable");
    }

    private void requireNetworkTopologyStrategy(String keyspaceName) throws InvalidRequestException
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
        if (!(strategy instanceof NetworkTopologyStrategy))
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", this, strategy.getClass().getName()));
    }
}
