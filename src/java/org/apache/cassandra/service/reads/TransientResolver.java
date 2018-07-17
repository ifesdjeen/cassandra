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

package org.apache.cassandra.service.reads;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.ReplicaSet;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.reads.repair.PartitionIteratorMergeListener;
import org.apache.cassandra.service.reads.repair.ReadRepair;

/**
 * We need to do a few things with digest reads that include transient data
 * 1. send repairs to full replicas if the transient replica has data they don't
 * 2. forward repair mutations to full replicas that sent digest responses (and therefore
 *    weren't involved in the data resolution process)
 * 3. in cases where we receive multiple full data responses from a speculative retry, avoid
 *    comparing data responses we already know are identical from the digest comparisons
 * 4. don't add any overhead to non-transient reads
 * 5. Use the same responses in the data resolution used in the digest comparisons
 *
 * This class assumes that all of the responses from full replicas agreed on their data (otherwise
 * we'd be doing a normal foreground repair)
 */
class TransientResolver
{
    private final List<MessageIn<ReadResponse>> responses;
    private final ReplicaSet forwardTo;
    private final Replica dataReplica;
    private final ReplicaList participants;
    private final DigestResolver resolver;

    TransientResolver(Replica dataReplica, DigestResolver resolver)
    {
        this.responses = new ArrayList<>(resolver.maxResponseCount);
        this.forwardTo = new ReplicaSet(resolver.maxResponseCount);
        this.dataReplica = dataReplica;
        this.participants = new ReplicaList(resolver.maxResponseCount);
        this.resolver = resolver;
    }

    void addResponse(MessageIn<ReadResponse> response)
    {
        Replica replica = resolver.getReplicaFor(response.from);
        if (replica.isTransient() || replica.equals(dataReplica))
        {
            Preconditions.checkArgument(!response.payload.isDigestResponse());
            responses.add(response);
            participants.add(replica);
        }
        else
        {
            forwardTo.add(replica);
        }
    }

    PartitionIterator resolve()
    {
        DataResolver dataResolver = new DataResolver(resolver.keyspace,
                                                     resolver.command,
                                                     resolver.consistency,
                                                     participants,
                                                     new ForwardingReadRepair(),
                                                     resolver.maxResponseCount,
                                                     resolver.queryStartNanoTime);

        for (MessageIn<ReadResponse> response : responses)
            dataResolver.preprocess(response);

        return dataResolver.resolve();
    }

    private class ForwardingReadRepair implements ReadRepair
    {
        @Override
        public UnfilteredPartitionIterators.MergeListener getMergeListener(ReplicaList replicas)
        {
            return new PartitionIteratorMergeListener(replicas, resolver.command, resolver.consistency, this);
        }

        @Override
        public void startRepair(DigestResolver digestResolver, ReplicaList allReplicas, ReplicaList contactedReplicas, Consumer<PartitionIterator> resultConsumer)
        {
            throw new IllegalStateException("Transient data merge repairs cannot perform reads");
        }

        @Override
        public void awaitRepair() throws ReadTimeoutException
        {
            throw new IllegalStateException("Transient data merge repairs cannot perform reads");
        }

        @Override
        public void maybeSendAdditionalDataRequests()
        {
            throw new IllegalStateException("Transient data merge repairs cannot perform reads");
        }

        @Override
        public void maybeSendAdditionalRepairs()
        {
            resolver.readRepair.maybeSendAdditionalRepairs();
        }

        @Override
        public void awaitRepairs()
        {
            resolver.readRepair.awaitRepairs();
        }

        @Override
        public void repairPartition(DecoratedKey key, Map<Replica, Mutation> mutations, ReplicaList destinations)
        {
            Preconditions.checkArgument(mutations.containsKey(dataReplica));

            Mutation mutation = mutations.get(dataReplica);
            for (Replica digestSender: forwardTo)
                mutations.put(digestSender, mutation);

            resolver.readRepair.repairPartition(key, mutations, destinations);
        }
    }
}
