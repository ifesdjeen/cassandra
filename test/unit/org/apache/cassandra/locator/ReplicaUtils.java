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

import java.net.UnknownHostException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;

public class ReplicaUtils
{
    public static final Range<Token> FULL_RANGE = new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM);
    public static final AbstractBounds<PartitionPosition> FULL_BOUNDS = new Range<>(Murmur3Partitioner.MINIMUM.minKeyBound(), Murmur3Partitioner.MINIMUM.maxKeyBound());

    public static Replica full(InetAddressAndPort endpoint)
    {
        return fullReplica(endpoint, FULL_RANGE);
    }

    public static Replica full(String name)
    {
        try
        {
            return ReplicaUtils.full(InetAddressAndPort.getByName(name));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    public static Replica trans(InetAddressAndPort endpoint)
    {
        return transientReplica(endpoint, FULL_RANGE);
    }

    public static Token token(int token)
    {
        return DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(token));
    }

    public static ReplicaPlan.SharedForTokenRead sharedPlan(Keyspace ks, ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, consistencyLevel, replicas, replicas));
    }


    static ReplicaPlan.ForRangeRead replicaPlan(Keyspace ks, ConsistencyLevel consistencyLevel, EndpointsForRange replicas)
    {
        return replicaPlan(ks, consistencyLevel, replicas, replicas);
    }

    static ReplicaPlan.ForRangeRead replicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForRange replicas, EndpointsForRange targets)
    {
        return new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel,
                                            ReplicaUtils.FULL_BOUNDS, replicas, targets);
    }

}
