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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.reads.repair.PartitionIteratorMergeListener;
import org.apache.cassandra.service.reads.repair.ReadRepair;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class DigestResolver<E extends Endpoints<E>, L extends ReplicaLayout<E, L>> extends ResponseResolver<E, L>
{
    private volatile MessageIn<ReadResponse> dataResponse;
    private volatile boolean hasTransientResponse = false;

    public DigestResolver(ReadCommand command, L replicas, ReadRepair<E, L> readRepair, long queryStartNanoTime)
    {
        super(command, replicas, readRepair, queryStartNanoTime);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        Replica replica = replicaLayout.getReplicaFor(message.from);
        if (dataResponse == null && !message.payload.isDigestResponse() && replica.isFull())
        {
            dataResponse = message;
        }
        else if (replica.isTransient())
        {
            Preconditions.checkArgument(!message.payload.isDigestResponse(), "digest response received from transient replica");
            hasTransientResponse = true;
        }
    }

    public PartitionIterator getData()
    {
        assert isDataPresent();

        if (!hasTransientResponse)
        {
            return UnfilteredPartitionIterators.filter(dataResponse.payload.makeIterator(command), command.nowInSec());
        }
        else
        {
            // This path can be triggered only if we've got responses from full replicas and they match, but
            // transient replica response still contains data, which needs to be reconciled.
            // Create data resolver that will forward data
            L layout = replicaLayout.forResponded(transform(
                    filter(responses, a -> a.payload.isDigestResponse()),
                    a -> a.from));
            DataResolver<E, L> dataResolver = new DataResolver<>(command,
                                                                 replicaLayout,
                                                                 new ForwardingReadRepair(replicaLayout.getReplicaFor(dataResponse.from),
                                                                                          layout),
                                                                 queryStartNanoTime);

            dataResolver.preprocess(dataResponse);
            // Forward differences to all full nodes
            for (MessageIn<ReadResponse> response : responses)
            {
                Replica replica = replicaLayout.getReplicaFor(response.from);
                if (replica.isTransient())
                    dataResolver.preprocess(response);
            }

            return dataResolver.resolve();
        }
    }

    public boolean responsesMatch()
    {
        long start = System.nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            if (replicaLayout.getReplicaFor(message.from).isTransient())
                continue;

            ByteBuffer newDigest = message.payload.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return true;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }

    @VisibleForTesting
    public boolean hasTransientResponse()
    {
        return hasTransientResponse;
    }

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
    private class ForwardingReadRepair implements ReadRepair<E, L>
    {
        private final Replica from;
        private final L forwardTo;

        public ForwardingReadRepair(Replica from, L forwardTo)
        {
            this.from = from;
            this.forwardTo = forwardTo;
        }

        @Override
        public UnfilteredPartitionIterators.MergeListener getMergeListener(L replicas)
        {
            // TODO: consistencylevel here is probably redundant by now
            return new PartitionIteratorMergeListener(replicas, command, replicaLayout.consistencyLevel(), this);
        }

        @Override
        public void startRepair(DigestResolver<E, L> digestResolver, Consumer<PartitionIterator> resultConsumer)
        {
            throw new IllegalStateException("Transient data merge repairs cannot perform reads");
        }

        @Override
        public void awaitReads() throws ReadTimeoutException
        {
            throw new IllegalStateException("Transient data merge repairs cannot perform reads");
        }

        @Override
        public void maybeSendAdditionalReads()
        {
            throw new IllegalStateException("Transient data merge repairs cannot perform reads");
        }

        @Override
        public void maybeSendAdditionalWrites()
        {
            readRepair.maybeSendAdditionalWrites();
        }

        @Override
        public void awaitWrites()
        {
            readRepair.awaitWrites();
        }

        @Override
        public void repairPartition(Map<Replica, Mutation> mutations, L replicaLayout)
        {
            Preconditions.checkArgument(mutations.containsKey(from));

            Mutation mutation = mutations.get(from);
            for (Replica digestSender: forwardTo.selectedReplicas())
            {
                mutations.put(digestSender, mutation);
            }

            readRepair.repairPartition(mutations, replicaLayout);
        }
    }
}
