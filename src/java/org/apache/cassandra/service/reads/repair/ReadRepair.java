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
package org.apache.cassandra.service.reads.repair;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.service.reads.DigestResolver;

public interface ReadRepair
{
    /**
     * Used by DataResolver to generate corrections as the partition iterator is consumed
     */
    UnfilteredPartitionIterators.MergeListener getMergeListener(ReplicaList replicas);

    /**
     * Called when the digests from the initial read don't match. Reads may block on the
     * repair started by this method.
     */
    public void startRepair(DigestResolver digestResolver,
                            ReplicaList allEndpoints,
                            ReplicaList contactedEndpoints,
                            Consumer<PartitionIterator> resultConsumer);

    /**
     * Wait for any operations started by {@link ReadRepair#startRepair} to complete
     */
    public void awaitRepair() throws ReadTimeoutException;

    /**
     * if it looks like we might not receive data requests from everyone in time, send additional requests
     * to additional replicas not contacted in the initial full data read. If the collection of nodes that
     * end up responding in time end up agreeing on the data, and we don't consider the response from the
     * disagreeing replica that triggered the read repair, that's ok, since the disagreeing data would not
     * have been successfully written and won't be included in the response the the client, preserving the
     * expectation of monotonic quorum reads
     */
    public void maybeSendAdditionalDataRequests();

    /**
     * If it looks like we might not receive acks for all the repair mutations we sent out, combine all
     * the unacked mutations and send them to the minority of nodes not involved in the read repair data
     * read / write cycle. We will accept acks from them in lieu of acks from the initial mutations sent
     * out, so long as we receive the same number of acks as repair mutations transmitted. This prevents
     * misbehaving nodes from killing a quorum read, while continuing to guarantee monotonic quorum reads
     */
    public void maybeSendAdditionalRepairs();

    public void awaitRepairs();

    void repairPartition(DecoratedKey key, Map<Replica, Mutation> mutations, ReplicaList destinations);

    static ReadRepair create(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        return new BlockingReadRepair(command, queryStartNanoTime, consistency);
    }
}
