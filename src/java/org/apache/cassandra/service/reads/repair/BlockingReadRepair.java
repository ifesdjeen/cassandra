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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ReplicaPlan;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements ReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final ReadCommand command;
    private final long queryStartNanoTime;
    private final ColumnFamilyStore cfs;
    private final ReplicaPlan replicaPlan;

    private final Queue<BlockingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    private volatile DigestRepair digestRepair = null;

    private boolean blockingRepairReported = false;
    private final int blockFor;

    private static class DigestRepair
    {
        private final DataResolver dataResolver;
        private final ReadCallback readCallback;
        private final Consumer<PartitionIterator> resultConsumer;

        public DigestRepair(DataResolver dataResolver, ReadCallback readCallback, Consumer<PartitionIterator> resultConsumer)
        {
            this.dataResolver = dataResolver;
            this.readCallback = readCallback;
            this.resultConsumer = resultConsumer;
        }
    }

    public BlockingReadRepair(ReadCommand command,
                              ReplicaPlan replicaPlan,
                              long queryStartNanoTime)
    {
        this.command = command;
        this.replicaPlan = replicaPlan;
        this.queryStartNanoTime = queryStartNanoTime;
        this.cfs = Keyspace.openAndGetStore(command.metadata());
        this.blockFor = replicaPlan.consistencyLevel().blockFor(cfs.keyspace);
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(ReplicaList replicas)
    {
        return new PartitionIteratorMergeListener(replicas, command, replicaPlan.consistencyLevel(), this);
    }

    /**
     * Blocking repairs can be started by a digest mismatch for single partition reads, merged data
     * on a partition range read, or transient data being repaired to full replicas. This makes sure
     * we only report one blocking repair for each read repair instance.
     */
    private void maybeMarkBlockingRepair()
    {
        if (!blockingRepairReported)
        {
            ReadRepairMetrics.repairedBlocking.mark();
            blockingRepairReported = true;
        }
    }

    public void startRepair(DigestResolver digestResolver, Consumer<PartitionIterator> resultConsumer)
    {
        maybeMarkBlockingRepair();

        // Do a full data read to resolve the correct response (and repair node that need be)
        ReplicaPlan repairPlan = replicaPlan.with(ConsistencyLevel.ALL);
        DataResolver resolver = new DataResolver(command, repairPlan, this, queryStartNanoTime);
        ReadCallback readCallback = new ReadCallback(resolver, repairPlan.targetReplicas().size(), command, cfs.keyspace, repairPlan, queryStartNanoTime);
        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);

        for (Replica replica : repairPlan.targetReplicas())
        {
            Tracing.trace("Enqueuing full data read to {}", replica);
            MessagingService.instance().sendRRWithFailure(command.createMessage(), replica.getEndpoint(), readCallback);
        }
    }

    public void awaitRepair() throws ReadTimeoutException
    {
        DigestRepair repair = digestRepair;
        if (repair == null)
            return;

        repair.readCallback.awaitResults();
        repair.resultConsumer.accept(digestRepair.dataResolver.resolve());
    }

    boolean shouldSpeculate()
    {
        ConsistencyLevel consistency = replicaPlan.consistencyLevel();
        ConsistencyLevel speculativeCL = consistency.isDatacenterLocal() ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;
        return  consistency != ConsistencyLevel.EACH_QUORUM
               && consistency.satisfies(speculativeCL, cfs.keyspace)
               && cfs.sampleReadLatencyNanos <= TimeUnit.MILLISECONDS.toNanos(command.getTimeout());
    }

    public void maybeSendAdditionalDataRequests()
    {
        DigestRepair repair = digestRepair;
        if (repair == null)
            return;

        if (shouldSpeculate() && !repair.readCallback.await(cfs.sampleReadLatencyNanos, TimeUnit.NANOSECONDS))
        {
            boolean speculated = false;
            for (Replica replica: replicaPlan.additionalReplicas())
            {
                speculated = true;
                Tracing.trace("Enqueuing speculative full data read to {}", replica);
                MessagingService.instance().sendRR(command.createMessage(), replica.getEndpoint(), repair.readCallback);
            }

            if (speculated)
                ReadRepairMetrics.speculatedRead.mark();
        }
    }

    @Override
    public void maybeSendAdditionalRepairs()
    {
        for (BlockingPartitionRepair repair: repairs)
        {
            repair.maybeSendAdditionalRepairs(cfs.sampleReadLatencyNanos, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void awaitRepairs()
    {
        boolean timedOut = false;
        for (BlockingPartitionRepair repair: repairs)
        {
            if (!repair.awaitRepairs(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
            {
                timedOut = true;
            }
        }
        if (timedOut)
        {
            // We got all responses, but timed out while repairing
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(replicaPlan.consistencyLevel(), blockFor - 1, blockFor, true);
        }
    }

    @Override
    public void repairPartition(Map<Replica, Mutation> mutations, ReplicaList targets)
    {
        maybeMarkBlockingRepair();
        // Targets are overriden with ones that have responsed
        BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(mutations, blockFor, replicaPlan.with(targets));
        blockingRepair.sendInitialRepairs();
        repairs.add(blockingRepair);
    }
}
