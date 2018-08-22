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

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.service.reads.AbstractReadExecutor;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.repair.BlockingReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.ViewUtils;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.metrics.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.PrepareCallback;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeCallback;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.AbstractIterator;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private static volatile int maxHintsInProgress = 128 * FBUtilities.getAvailableProcessors();
    private static final CacheLoader<InetAddressAndPort, AtomicInteger> hintsInProgress = new CacheLoader<InetAddressAndPort, AtomicInteger>()
    {
        public AtomicInteger load(InetAddressAndPort inetAddress)
        {
            return new AtomicInteger(0);
        }
    };
    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
    private static final ClientWriteRequestMetrics writeMetrics = new ClientWriteRequestMetrics("Write");
    private static final CASClientWriteRequestMetrics casWriteMetrics = new CASClientWriteRequestMetrics("CASWrite");
    private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("CASRead");
    private static final ViewWriteMetrics viewWriteMetrics = new ViewWriteMetrics("ViewWrite");
    private static final Map<ConsistencyLevel, ClientRequestMetrics> readMetricsMap = new EnumMap<>(ConsistencyLevel.class);
    private static final Map<ConsistencyLevel, ClientWriteRequestMetrics> writeMetricsMap = new EnumMap<>(ConsistencyLevel.class);

    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    private StorageProxy()
    {
    }

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        HintsService.instance.registerMBean();
        HintedHandOffManager.instance.registerMBean();

        standardWritePerformer = (mutation, targets, responseHandler, localDataCenter, consistency_level) ->
        {
            assert mutation instanceof Mutation;
            sendToHintedReplicas((Mutation) mutation, targets, responseHandler, localDataCenter, Stage.MUTATION);
        };

        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the COUNTER_MUTATION stage
         * but on the latter case, the verb handler already run on the COUNTER_MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = (mutation, targets, responseHandler, localDataCenter, consistencyLevel) ->
        {
            // TODO: test counters
            Replicas.checkFull(targets.selectedReplicas());
            counterWriteTask(mutation, targets, responseHandler, localDataCenter).run();
        };

        counterWriteOnCoordinatorPerformer = (mutation, targets, responseHandler, localDataCenter, consistencyLevel) ->
        {
            // TODO: test counters
            Replicas.checkFull(targets.selectedReplicas());
            StageManager.getStage(Stage.COUNTER_MUTATION)
                        .execute(counterWriteTask(mutation, targets, responseHandler, localDataCenter));
        };

        for(ConsistencyLevel level : ConsistencyLevel.values())
        {
            readMetricsMap.put(level, new ClientRequestMetrics("Read-" + level.name()));
            writeMetricsMap.put(level, new ClientWriteRequestMetrics("Write-" + level.name()));
        }
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @param keyspaceName the keyspace for the CAS
     * @param cfName the column family for the CAS
     * @param key the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static RowIterator cas(String keyspaceName,
                                  String cfName,
                                  DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit,
                                  ClientState state,
                                  long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        final long startTimeForMetrics = System.nanoTime();
        int contentions = 0;
        try
        {
            consistencyForPaxos.validateForCas();
            consistencyForCommit.validateForCasCommit(keyspaceName);

            TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, cfName);

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
            while (System.nanoTime() - queryStartNanoTime < timeout)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                ReplicaLayout.ForPaxos replicaLayout = ReplicaLayout.forPaxos(Keyspace.open(keyspaceName), key, consistencyForPaxos);

                final PaxosBallotAndContention pair = beginAndRepairPaxos(queryStartNanoTime, key, metadata, replicaLayout, consistencyForPaxos, consistencyForCommit, true, state);
                final UUID ballot = pair.ballot;
                contentions += pair.contentions;

                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");
                SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) request.readCommand(FBUtilities.nowInSeconds());
                ConsistencyLevel readConsistency = consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;

                FilteredPartition current;
                try (RowIterator rowIter = readOne(readCommand, readConsistency, queryStartNanoTime))
                {
                    current = FilteredPartition.create(rowIter);
                }

                if (!request.appliesTo(current))
                {
                    Tracing.trace("CAS precondition does not match current values {}", current);
                    casWriteMetrics.conditionNotMet.inc();
                    return current.rowIterator();
                }

                // finish the paxos round w/ the desired updates
                // TODO turn null updates into delete?
                PartitionUpdate updates = request.makeUpdates(current);

                long size = updates.dataSize();
                casWriteMetrics.mutationSize.update(size);
                writeMetricsMap.get(consistencyForPaxos).mutationSize.update(size);

                // Apply triggers to cas updates. A consideration here is that
                // triggers emit Mutations, and so a given trigger implementation
                // may generate mutations for partitions other than the one this
                // paxos round is scoped for. In this case, TriggerExecutor will
                // validate that the generated mutations are targetted at the same
                // partition as the initial updates and reject (via an
                // InvalidRequestException) any which aren't.
                updates = TriggerExecutor.instance.execute(updates);


                Commit proposal = Commit.newProposal(ballot, updates);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                if (proposePaxos(proposal, replicaLayout, true, queryStartNanoTime))
                {
                    commitPaxos(proposal, consistencyForCommit, true, queryStartNanoTime);
                    Tracing.trace("CAS successful");
                    return null;
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // continue to retry
            }

            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
        }
        catch (WriteTimeoutException|ReadTimeoutException e)
        {
            casWriteMetrics.timeouts.mark();
            writeMetricsMap.get(consistencyForPaxos).timeouts.mark();
            throw e;
        }
        catch (WriteFailureException|ReadFailureException e)
        {
            casWriteMetrics.failures.mark();
            writeMetricsMap.get(consistencyForPaxos).failures.mark();
            throw e;
        }
        catch(UnavailableException e)
        {
            casWriteMetrics.unavailables.mark();
            writeMetricsMap.get(consistencyForPaxos).unavailables.mark();
            throw e;
        }
        finally
        {
            recordCasContention(contentions);
            final long latency = System.nanoTime() - startTimeForMetrics;
            casWriteMetrics.addNano(latency);
            writeMetricsMap.get(consistencyForPaxos).addNano(latency);
        }
    }

    private static void recordCasContention(int contentions)
    {
        if(contentions > 0)
            casWriteMetrics.contention.update(contentions);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static PaxosBallotAndContention beginAndRepairPaxos(long queryStartNanoTime,
                                                                DecoratedKey key,
                                                                TableMetadata metadata,
                                                                ReplicaLayout.ForPaxos replicaLayout,
                                                                ConsistencyLevel consistencyForPaxos,
                                                                ConsistencyLevel consistencyForCommit,
                                                                final boolean isWrite,
                                                                ClientState state)
    throws WriteTimeoutException, WriteFailureException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

        PrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - queryStartNanoTime < timeout)
        {
            // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
            // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
            // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
            // out-of-order (#7801).
            long minTimestampMicrosToUse = summary == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(summary.mostRecentInProgressCommit.ballot);
            long ballotMicros = state.getTimestampForPaxos(minTimestampMicrosToUse);
            // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled concurrently by the same coordinator. But we still
            // need ballots to be unique for each proposal so we have to use getRandomTimeUUIDFromMicros.
            UUID ballot = UUIDGen.getRandomTimeUUIDFromMicros(ballotMicros);

            // prepare
            Tracing.trace("Preparing {}", ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            summary = preparePaxos(toPrepare, replicaLayout, queryStartNanoTime);
            if (!summary.promised)
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                contentions++;
                // sleep a random amount to give the other proposer a chance to finish
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                continue;
            }

            Commit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            Commit mostRecent = summary.mostRecentCommit;

            // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
            // needs to be completed, so do it.
            if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
            {
                Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                if(isWrite)
                    casWriteMetrics.unfinishedCommit.inc();
                else
                    casReadMetrics.unfinishedCommit.inc();
                Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update);
                if (proposePaxos(refreshedInProgress, replicaLayout, false, queryStartNanoTime))
                {
                    try
                    {
                        commitPaxos(refreshedInProgress, consistencyForCommit, false, queryStartNanoTime);
                    }
                    catch (WriteTimeoutException e)
                    {
                        recordCasContention(contentions);
                        // We're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                        throw new WriteTimeoutException(WriteType.CAS, e.consistency, e.received, e.blockFor);
                    }
                }
                else
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    contentions++;
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
                continue;
            }

            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes, and retry.
            int nowInSec = Ints.checkedCast(TimeUnit.MICROSECONDS.toSeconds(ballotMicros));
            Iterable<InetAddressAndPort> missingMRC = summary.replicasMissingMostRecentCommit(metadata, nowInSec);
            if (Iterables.size(missingMRC) > 0)
            {
                Tracing.trace("Repairing replicas that missed the most recent commit");
                sendCommit(mostRecent, missingMRC);
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                continue;
            }

            return new PaxosBallotAndContention(ballot, contentions);
        }

        recordCasContention(contentions);
        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.keyspace)));
    }

    /**
     * Unlike commitPaxos, this does not wait for replies
     */
    private static void sendCommit(Commit commit, Iterable<InetAddressAndPort> replicas)
    {
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, commit, Commit.serializer);
        for (InetAddressAndPort target : replicas)
            MessagingService.instance().sendOneWay(message, target);
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, ReplicaLayout.ForPaxos replicaLayout, long queryStartNanoTime)
    throws WriteTimeoutException
    {
        PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(), toPrepare.update.metadata(), replicaLayout.getRequiredParticipants(), replicaLayout.consistencyLevel(), queryStartNanoTime);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, Commit.serializer);
        for (Replica replica: replicaLayout.selectedReplicas())
        {
            if (replica.isLocal())
            {
                StageManager.getStage(MessagingService.verbStages.get(MessagingService.Verb.PAXOS_PREPARE)).execute(new Runnable()
                {
                    public void run()
                    {
                        try
                        {
                            MessageIn<PrepareResponse> message = MessageIn.create(FBUtilities.getBroadcastAddressAndPort(),
                                    PrepareVerbHandler.doPrepare(toPrepare),
                                    Collections.emptyMap(),
                                    MessagingService.Verb.INTERNAL_RESPONSE,
                                    MessagingService.current_version);
                            callback.response(message);
                        }
                        catch (Exception ex)
                        {
                            logger.error("Failed paxos prepare locally", ex);
                        }
                    }
                });
            }
            else
            {
                MessagingService.instance().sendRR(message, replica.endpoint(), callback);
            }
        }
        callback.await();
        return callback;
    }

    private static boolean proposePaxos(Commit proposal, ReplicaLayout.ForPaxos replicaLayout, boolean timeoutIfPartial, long queryStartNanoTime)
    throws WriteTimeoutException
    {
        ProposeCallback callback = new ProposeCallback(replicaLayout.selectedReplicas().size(), replicaLayout.getRequiredParticipants(), !timeoutIfPartial, replicaLayout.consistencyLevel(), queryStartNanoTime);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PROPOSE, proposal, Commit.serializer);
        for (Replica replica : replicaLayout.selectedReplicas())
        {
            if (replica.isLocal())
            {
                StageManager.getStage(MessagingService.verbStages.get(MessagingService.Verb.PAXOS_PROPOSE)).execute(new Runnable()
                {
                    public void run()
                    {
                        try
                        {
                            MessageIn<Boolean> message = MessageIn.create(FBUtilities.getBroadcastAddressAndPort(),
                                    ProposeVerbHandler.doPropose(proposal),
                                    Collections.emptyMap(),
                                    MessagingService.Verb.INTERNAL_RESPONSE,
                                    MessagingService.current_version);
                            callback.response(message);
                        }
                        catch (Exception ex)
                        {
                            logger.error("Failed paxos propose locally", ex);
                        }
                    }
                });
            }
            else
            {
                MessagingService.instance().sendRR(message, replica.endpoint(), callback);
            }
        }
        callback.await();

        if (callback.isSuccessful())
            return true;

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, replicaLayout.consistencyLevel(), callback.getAcceptCount(), replicaLayout.getRequiredParticipants());

        return false;
    }

    private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, long queryStartNanoTime) throws WriteTimeoutException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().keyspace);

        Token tk = proposal.update.partitionKey().getToken();

        AbstractWriteResponseHandler<Commit> responseHandler = null;
        ReplicaLayout.ForToken replicaPlan = ReplicaLayout.forWrite(keyspace, consistencyLevel, tk, FailureDetector.instance::isAlive);
        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            responseHandler = rs.getWriteResponseHandler(replicaPlan, null, WriteType.SIMPLE, queryStartNanoTime);
            responseHandler.setSupportsBackPressure(false);
        }

        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, proposal, Commit.serializer);
        for (Replica replica : replicaPlan.allReplicas())
        {
            InetAddressAndPort destination = replica.endpoint();
            checkHintOverload(replica);

            if (FailureDetector.instance.isAlive(destination))
            {
                if (shouldBlock)
                {
                    if (replica.isLocal())
                        commitPaxosLocal(replica, message, responseHandler);
                    else
                        MessagingService.instance().sendWriteRR(message, replica, responseHandler, allowHints && shouldHint(replica));
                }
                else
                {
                    MessagingService.instance().sendOneWay(message, destination);
                }
            }
            else
            {
                if (responseHandler != null)
                {
                    responseHandler.expired();
                }
                if (allowHints && shouldHint(replica))
                {
                    submitHint(proposal.makeMutation(), replica, null);
                }
            }
        }

        if (shouldBlock)
            responseHandler.get();
    }

    /**
     * Commit a PAXOS task locally, and if the task times out rather then submitting a real hint
     * submit a fake one that executes immediately on the mutation stage, but generates the necessary backpressure
     * signal for hints
     */
    private static void commitPaxosLocal(Replica localReplica, final MessageOut<Commit> message, final AbstractWriteResponseHandler<?> responseHandler)
    {
        StageManager.getStage(MessagingService.verbStages.get(MessagingService.Verb.PAXOS_COMMIT)).maybeExecuteImmediately(new LocalMutationRunnable(localReplica)
        {
            public void runMayThrow()
            {
                try
                {
                    PaxosState.commit(message.payload);
                    if (responseHandler != null)
                        responseHandler.response(null);
                }
                catch (Exception ex)
                {
                    if (!(ex instanceof WriteTimeoutException))
                        logger.error("Failed to apply paxos commit locally : ", ex);
                    responseHandler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
                }
            }

            @Override
            protected Verb verb()
            {
                return MessagingService.Verb.PAXOS_COMMIT;
            }
        });
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static void mutate(List<? extends IMutation> mutations, ConsistencyLevel consistency_level, long queryStartNanoTime)
    throws UnavailableException, OverloadedException, WriteTimeoutException, WriteFailureException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());

        long startTime = System.nanoTime();

        List<AbstractWriteResponseHandler<IMutation>> responseHandlers = new ArrayList<>(mutations.size());
        WriteType plainWriteType = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        try
        {
            for (IMutation mutation : mutations)
            {
                if (mutation instanceof CounterMutation)
                    responseHandlers.add(mutateCounter((CounterMutation)mutation, localDataCenter, queryStartNanoTime));
                else
                    responseHandlers.add(performWrite(mutation, consistency_level, localDataCenter, standardWritePerformer, null, plainWriteType, queryStartNanoTime));
            }

            // upgrade to full quorum any failed cheap quorums
            for (int i = 0 ; i < mutations.size() ; ++i)
            {
                if (!(mutations.get(i) instanceof CounterMutation)) // at the moment, only non-counter writes support cheap quorums
                    responseHandlers.get(i).maybeTryAdditionalReplicas(mutations.get(i), standardWritePerformer, localDataCenter);
            }

            // wait for writes.  throws TimeoutException if necessary
            for (AbstractWriteResponseHandler<IMutation> responseHandler : responseHandlers)
                responseHandler.get();
        }
        catch (WriteTimeoutException|WriteFailureException ex)
        {
            if (consistency_level == ConsistencyLevel.ANY)
            {
                hintMutations(mutations);
            }
            else
            {
                if (ex instanceof WriteFailureException)
                {
                    writeMetrics.failures.mark();
                    writeMetricsMap.get(consistency_level).failures.mark();
                    WriteFailureException fe = (WriteFailureException)ex;
                    Tracing.trace("Write failure; received {} of {} required replies, failed {} requests",
                                  fe.received, fe.blockFor, fe.failureReasonByEndpoint.size());
                }
                else
                {
                    writeMetrics.timeouts.mark();
                    writeMetricsMap.get(consistency_level).timeouts.mark();
                    WriteTimeoutException te = (WriteTimeoutException)ex;
                    Tracing.trace("Write timeout; received {} of {} required replies", te.received, te.blockFor);
                }
                throw ex;
            }
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            writeMetricsMap.get(consistency_level).unavailables.mark();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (OverloadedException e)
        {
            writeMetrics.unavailables.mark();
            writeMetricsMap.get(consistency_level).unavailables.mark();
            Tracing.trace("Overloaded");
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - startTime;
            writeMetrics.addNano(latency);
            writeMetricsMap.get(consistency_level).addNano(latency);
            updateCoordinatorWriteLatencyTableMetric(mutations, latency);
        }
    }

    /**
     * Hint all the mutations (except counters, which can't be safely retried).  This means
     * we'll re-hint any successful ones; doesn't seem worth it to track individual success
     * just for this unusual case.
     *
     * Only used for CL.ANY
     *
     * @param mutations the mutations that require hints
     */
    private static void hintMutations(Collection<? extends IMutation> mutations)
    {
        for (IMutation mutation : mutations)
            if (!(mutation instanceof CounterMutation))
                hintMutation((Mutation) mutation);

        Tracing.trace("Wrote hints to satisfy CL.ANY after no replicas acknowledged the write");
    }

    private static void hintMutation(Mutation mutation)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();

        // local writes can timeout, but cannot be dropped (see LocalMutationRunnable and CASSANDRA-6510),
        // so there is no need to hint or retry.
        EndpointsForToken replicasToHint = StorageService.instance.getNaturalAndPendingReplicasForToken(keyspaceName, token)
                .filter(target -> !target.isLocal() && shouldHint(target));

        submitHint(mutation, replicasToHint, null);
    }

    public boolean appliesLocally(Mutation mutation)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();

        return StorageService.instance.getNaturalReplicasForToken(keyspaceName, token).endpoints().contains(local)
               || StorageService.instance.getTokenMetadata().pendingEndpointsForToken(token, keyspaceName).endpoints().contains(local);
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param writeCommitLog if commitlog should be written
     * @param baseComplete time from epoch in ms that the local base mutation was(or will be) completed
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static void mutateMV(ByteBuffer dataKey, Collection<Mutation> mutations, boolean writeCommitLog, AtomicLong baseComplete, long queryStartNanoTime)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());

        long startTime = System.nanoTime();


        try
        {
            // if we haven't joined the ring, write everything to batchlog because paired replicas may be stale
            final UUID batchUUID = UUIDGen.getTimeUUID();

            if (StorageService.instance.isStarting() || StorageService.instance.isJoining() || StorageService.instance.isMoving())
            {
                BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(),
                                                        mutations), writeCommitLog);
            }
            else
            {
                List<WriteResponseHandlerWrapper> wrappers = new ArrayList<>(mutations.size());
                //non-local mutations rely on the base mutation commit-log entry for eventual consistency
                Set<Mutation> nonLocalMutations = new HashSet<>(mutations);
                Token baseToken = StorageService.instance.getTokenMetadata().partitioner.getToken(dataKey);

                ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

                //Since the base -> view replication is 1:1 we only need to store the BL locally
                final Collection<InetAddressAndPort> batchlogEndpoints = Collections.singleton(FBUtilities.getBroadcastAddressAndPort());
                BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(mutations.size(), () -> asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID));

                // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
                for (Mutation mutation : mutations)
                {
                    String keyspaceName = mutation.getKeyspaceName();
                    Token tk = mutation.key().getToken();
                    Optional<Replica> pairedEndpoint = ViewUtils.getViewNaturalEndpoint(keyspaceName, baseToken, tk);
                    EndpointsForToken pendingReplicas = StorageService.instance.getTokenMetadata().pendingEndpointsForToken(tk, keyspaceName);

                    // if there are no paired endpoints there are probably range movements going on, so we write to the local batchlog to replay later
                    if (!pairedEndpoint.isPresent())
                    {
                        if (pendingReplicas.isEmpty())
                            logger.warn("Received base materialized view mutation for key {} that does not belong " +
                                        "to this node. There is probably a range movement happening (move or decommission)," +
                                        "but this node hasn't updated its ring metadata yet. Adding mutation to " +
                                        "local batchlog to be replayed later.",
                                        mutation.key());
                        continue;
                    }

                    // When local node is the endpoint we can just apply the mutation locally,
                    // unless there are pending endpoints, in which case we want to do an ordinary
                    // write so the view mutation is sent to the pending endpoint
                    if (pairedEndpoint.get().isLocal() && StorageService.instance.isJoined()
                        && pendingReplicas.isEmpty())
                    {
                        try
                        {
                            mutation.apply(writeCommitLog);
                            nonLocalMutations.remove(mutation);
                            cleanup.ackMutation();
                        }
                        catch (Exception exc)
                        {
                            logger.error("Error applying local view update to keyspace {}: {}", mutation.getKeyspaceName(), mutation);
                            throw exc;
                        }
                    }
                    else
                    {
                        wrappers.add(wrapViewBatchResponseHandler(mutation,
                                                                  consistencyLevel,
                                                                  consistencyLevel,
                                                                  EndpointsForToken.of(tk, pairedEndpoint.get()),
                                                                  baseComplete,
                                                                  WriteType.BATCH,
                                                                  cleanup,
                                                                  queryStartNanoTime));
                    }
                }

                // Apply to local batchlog memtable in this thread
                if (!nonLocalMutations.isEmpty())
                    BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), nonLocalMutations), writeCommitLog);

                // Perform remote writes
                if (!wrappers.isEmpty())
                    asyncWriteBatchedMutations(wrappers, localDataCenter, Stage.VIEW_MUTATION);
            }
        }
        finally
        {
            viewWriteMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mutateWithTriggers(List<? extends IMutation> mutations,
                                          ConsistencyLevel consistencyLevel,
                                          boolean mutateAtomically,
                                          long queryStartNanoTime)
    throws WriteTimeoutException, WriteFailureException, UnavailableException, OverloadedException, InvalidRequestException
    {
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);

        boolean updatesView = Keyspace.open(mutations.iterator().next().getKeyspaceName())
                              .viewManager
                              .updatesAffectView(mutations, true);

        long size = IMutation.dataSize(mutations);
        writeMetrics.mutationSize.update(size);
        writeMetricsMap.get(consistencyLevel).mutationSize.update(size);

        if (augmented != null)
            mutateAtomically(augmented, consistencyLevel, updatesView, queryStartNanoTime);
        else
        {
            if (mutateAtomically || updatesView)
                mutateAtomically((Collection<Mutation>) mutations, consistencyLevel, updatesView, queryStartNanoTime);
            else
                mutate(mutations, consistencyLevel, queryStartNanoTime);
        }
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     *      write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param mutations the Mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     * @param requireQuorumForRemove at least a quorum of nodes will see update before deleting batchlog
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static void mutateAtomically(Collection<Mutation> mutations,
                                        ConsistencyLevel consistency_level,
                                        boolean requireQuorumForRemove,
                                        long queryStartNanoTime)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for atomic batch");
        long startTime = System.nanoTime();

        List<WriteResponseHandlerWrapper> wrappers = new ArrayList<WriteResponseHandlerWrapper>(mutations.size());
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());

        if (mutations.stream().anyMatch(mutation -> Keyspace.open(mutation.getKeyspaceName()).getReplicationStrategy().getReplicationFactor().trans > 0))
            throw new AssertionError("Logged batches are unsupported with transient replication");

        try
        {

            // If we are requiring quorum nodes for removal, we upgrade consistency level to QUORUM unless we already
            // require ALL, or EACH_QUORUM. This is so that *at least* QUORUM nodes see the update.
            ConsistencyLevel batchConsistencyLevel = requireQuorumForRemove
                                                     ? ConsistencyLevel.QUORUM
                                                     : consistency_level;

            switch (consistency_level)
            {
                case ALL:
                case EACH_QUORUM:
                    batchConsistencyLevel = consistency_level;
            }

            final Collection<InetAddressAndPort> batchlogEndpoints = getBatchlogReplicas(localDataCenter, batchConsistencyLevel);
            final UUID batchUUID = UUIDGen.getTimeUUID();
            BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(mutations.size(),
                                                                                                          () -> asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID));

            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
            for (Mutation mutation : mutations)
            {
                WriteResponseHandlerWrapper wrapper = wrapBatchResponseHandler(mutation,
                                                                               consistency_level,
                                                                               batchConsistencyLevel,
                                                                               WriteType.BATCH,
                                                                               cleanup,
                                                                               queryStartNanoTime);
                // exit early if we can't fulfill the CL at this time.
                wrapper.handler.assureSufficientLiveNodes();
                wrappers.add(wrapper);
            }

            // write to the batchlog
            syncWriteToBatchlog(mutations, batchlogEndpoints, batchUUID, queryStartNanoTime);

            // now actually perform the writes and wait for them to complete
            syncWriteBatchedMutations(wrappers, localDataCenter, Stage.MUTATION);
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            writeMetricsMap.get(consistency_level).unavailables.mark();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (WriteTimeoutException e)
        {
            writeMetrics.timeouts.mark();
            writeMetricsMap.get(consistency_level).timeouts.mark();
            Tracing.trace("Write timeout; received {} of {} required replies", e.received, e.blockFor);
            throw e;
        }
        catch (WriteFailureException e)
        {
            writeMetrics.failures.mark();
            writeMetricsMap.get(consistency_level).failures.mark();
            Tracing.trace("Write failure; received {} of {} required replies", e.received, e.blockFor);
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - startTime;
            writeMetrics.addNano(latency);
            writeMetricsMap.get(consistency_level).addNano(latency);
            updateCoordinatorWriteLatencyTableMetric(mutations, latency);
        }
    }

    private static void updateCoordinatorWriteLatencyTableMetric(Collection<? extends IMutation> mutations, long latency)
    {
        if (null == mutations)
        {
            return;
        }

        try
        {
            //TODO: Avoid giving same latency number for each CF in each mutation in a given set of mutations
            //We could potentially pass a callback into performWrite. And add callback provision for mutateCounter or mutateAtomically (sendToHintedEndPoints)
            //However, Trade off between write metric per CF accuracy vs performance hit due to callbacks. Similar issue exists with CoordinatorReadLatency metric.
            mutations.forEach(mutation -> {
                mutation.getTableIds().forEach(tableId -> {
                    Keyspace.open(mutation.getKeyspaceName()).getColumnFamilyStore(tableId).metric.coordinatorWriteLatency.update(latency, TimeUnit.NANOSECONDS);
                });
            });
        }
        catch (Exception ex)
        {
            logger.warn("Exception occurred updating coordinatorWriteLatency metric", ex);
        }
    }

    private static void syncWriteToBatchlog(Collection<Mutation> mutations, Collection<InetAddressAndPort> endpoints, UUID uuid, long queryStartNanoTime)
    throws WriteTimeoutException, WriteFailureException
    {
        Keyspace systemKeypsace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        ReplicaLayout.ForToken replicaPlan = ReplicaLayout.forBatchlogWrite(systemKeypsace, endpoints);
        WriteResponseHandler<?> handler = new WriteResponseHandler(replicaPlan,
                                                                   WriteType.BATCH_LOG,
                                                                   queryStartNanoTime);

        Batch batch = Batch.createLocal(uuid, FBUtilities.timestampMicros(), mutations);
        MessageOut<Batch> message = new MessageOut<>(MessagingService.Verb.BATCH_STORE, batch, Batch.serializer);
        for (Replica replica : replicaPlan.selectedReplicas())
        {
            logger.trace("Sending batchlog store request {} to {} for {} mutations", batch.id, replica, batch.size());

            if (replica.isLocal())
                performLocally(Stage.MUTATION, replica, Optional.empty(), () -> BatchlogManager.store(batch), handler);
            else
                MessagingService.instance().sendRR(message, replica.endpoint(), handler);
        }
        handler.get();
    }

    private static void asyncRemoveFromBatchlog(Collection<InetAddressAndPort> endpoints, UUID uuid)
    {
        MessageOut<UUID> message = new MessageOut<>(MessagingService.Verb.BATCH_REMOVE, uuid, UUIDSerializer.serializer);
        for (InetAddressAndPort target : endpoints)
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending batchlog remove request {} to {}", uuid, target);

            if (target.equals(FBUtilities.getBroadcastAddressAndPort()))
                performLocally(Stage.MUTATION, SystemReplicas.getSystemReplica(target), () -> BatchlogManager.remove(uuid));
            else
                MessagingService.instance().sendOneWay(message, target);
        }
    }

    private static void asyncWriteBatchedMutations(List<WriteResponseHandlerWrapper> wrappers, String localDataCenter, Stage stage)
    {
        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Replicas.checkFull(wrapper.handler.replicaPlan.natrualReplicas());
            ReplicaLayout.ForToken replicas = wrapper.handler.replicaPlan.withSelected(wrapper.handler.replicaPlan.natrualReplicas());

            try
            {
                sendToHintedReplicas(wrapper.mutation, replicas, wrapper.handler, localDataCenter, stage);
            }
            catch (OverloadedException | WriteTimeoutException e)
            {
                wrapper.handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
            }
        }
    }

    private static void syncWriteBatchedMutations(List<WriteResponseHandlerWrapper> wrappers, String localDataCenter, Stage stage)
    throws WriteTimeoutException, OverloadedException
    {
        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Replicas.checkFull(wrapper.handler.replicaPlan.natrualReplicas());
            ReplicaLayout.ForToken replicaPlan = wrapper.handler.replicaPlan;

            sendToHintedReplicas(wrapper.mutation, replicaPlan.withSelected(replicaPlan.natrualReplicas()), wrapper.handler, localDataCenter, stage);
        }


        for (WriteResponseHandlerWrapper wrapper : wrappers)
            wrapper.handler.get();
    }

    /**
     * Perform the write of a mutation given a WritePerformer.
     * Gather the list of write endpoints, apply locally and/or forward the mutation to
     * said write endpoint (deletaged to the actual WritePerformer) and wait for the
     * responses based on consistency level.
     *
     * @param mutation the mutation to be applied
     * @param consistencyLevel the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     * @param callback an optional callback to be run if and when the write is
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static AbstractWriteResponseHandler<IMutation> performWrite(IMutation mutation,
                                                                       ConsistencyLevel consistencyLevel,
                                                                       String localDataCenter,
                                                                       WritePerformer performer,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       long queryStartNanoTime)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Keyspace keyspace = Keyspace.open(keyspaceName);
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();

        Token tk = mutation.key().getToken();

        ReplicaLayout.ForToken replicaPlan = ReplicaLayout.forWrite(keyspace, consistencyLevel, tk, FailureDetector.instance::isAlive);
        AbstractWriteResponseHandler<IMutation> responseHandler = rs.getWriteResponseHandler(replicaPlan, callback, writeType, queryStartNanoTime);

        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, replicaPlan, responseHandler, localDataCenter, consistencyLevel);
        return responseHandler;
    }

    // same as performWrites except does not initiate writes (but does perform availability checks).
    private static WriteResponseHandlerWrapper wrapBatchResponseHandler(Mutation mutation,
                                                                        ConsistencyLevel consistencyLevel,
                                                                        ConsistencyLevel batchConsistencyLevel,
                                                                        WriteType writeType,
                                                                        BatchlogResponseHandler.BatchlogCleanup cleanup,
                                                                        long queryStartNanoTime)
    {
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
        Token tk = mutation.key().getToken();

        ReplicaLayout.ForToken replicaLayout = ReplicaLayout.forWrite(keyspace, consistencyLevel, tk, FailureDetector.instance::isAlive);
        AbstractWriteResponseHandler<IMutation> writeHandler = rs.getWriteResponseHandler(replicaLayout,null, writeType, queryStartNanoTime);
        BatchlogResponseHandler<IMutation> batchHandler = new BatchlogResponseHandler<>(writeHandler, batchConsistencyLevel.blockFor(keyspace), cleanup, queryStartNanoTime);
        return new WriteResponseHandlerWrapper(batchHandler, mutation);
    }

    /**
     * Same as performWrites except does not initiate writes (but does perform availability checks).
     * Keeps track of ViewWriteMetrics
     */
    private static WriteResponseHandlerWrapper wrapViewBatchResponseHandler(Mutation mutation,
                                                                            ConsistencyLevel consistencyLevel,
                                                                            ConsistencyLevel batchConsistencyLevel,
                                                                            EndpointsForToken naturalEndpoints,
                                                                            AtomicLong baseComplete,
                                                                            WriteType writeType,
                                                                            BatchlogResponseHandler.BatchlogCleanup cleanup,
                                                                            long queryStartNanoTime)
    {
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
        String keyspaceName = mutation.getKeyspaceName();
        Token tk = mutation.key().getToken();
        EndpointsForToken pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsForToken(tk, keyspaceName);

        ReplicaLayout.ForToken plan = ReplicaLayout.forWrite(keyspace, consistencyLevel, tk, naturalEndpoints, naturalEndpoints, pendingEndpoints);

        AbstractWriteResponseHandler<IMutation> writeHandler = rs.getWriteResponseHandler(plan, () -> {
            long delay = Math.max(0, System.currentTimeMillis() - baseComplete.get());
            viewWriteMetrics.viewWriteLatency.update(delay, TimeUnit.MILLISECONDS);
        }, writeType, queryStartNanoTime);
        BatchlogResponseHandler<IMutation> batchHandler = new ViewWriteMetricsWrapped(writeHandler, batchConsistencyLevel.blockFor(keyspace), cleanup, queryStartNanoTime);
        return new WriteResponseHandlerWrapper(batchHandler, mutation);
    }

    // used by atomic_batch_mutate to decouple availability check from the write itself, caches consistency level and endpoints.
    private static class WriteResponseHandlerWrapper
    {
        final BatchlogResponseHandler<IMutation> handler;
        final Mutation mutation;

        WriteResponseHandlerWrapper(BatchlogResponseHandler<IMutation> handler, Mutation mutation)
        {
            this.handler = handler;
            this.mutation = mutation;
        }
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node DC
     */
    private static Collection<InetAddressAndPort> getBatchlogReplicas(String localDataCenter, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        Multimap<String, InetAddressAndPort> localEndpoints = HashMultimap.create(topology.getDatacenterRacks().get(localDataCenter));
        String localRack = DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddressAndPort());

        Collection<InetAddressAndPort> chosenEndpoints = new BatchlogManager.EndpointFilter(localRack, localEndpoints).filter();
        if (chosenEndpoints.isEmpty())
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return Collections.singleton(FBUtilities.getBroadcastAddressAndPort());

            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);
        }

        return chosenEndpoints;
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     * <pre>
     * {@code
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * }
     * </pre>
     *
     * @throws OverloadedException if the hints cannot be written/enqueued
     */
    public static void sendToHintedReplicas(final Mutation mutation,
                                            ReplicaLayout.ForToken targets,
                                            AbstractWriteResponseHandler<IMutation> responseHandler,
                                            String localDataCenter,
                                            Stage stage)
    throws OverloadedException
    {
        // this dc replicas:
        Collection<Replica> localDc = null;
        // extra-datacenter replicas, grouped by dc
        Map<String, Collection<Replica>> dcGroups = null;
        // only need to create a Message for non-local writes
        MessageOut<Mutation> message = null;

        boolean insertLocal = false;
        Replica localReplica = null;
        Collection<Replica> endpointsToHint = null;

        List<InetAddressAndPort> backPressureHosts = null;

        for (Replica destination : targets.selectedReplicas())
        {
            checkHintOverload(destination);

            if (FailureDetector.instance.isAlive(destination.endpoint()))
            {
                if (destination.isLocal())
                {
                    insertLocal = true;
                    localReplica = destination;
                }
                else
                {
                    // belongs on a different server
                    if (message == null)
                        message = mutation.createMessage();

                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);

                    // direct writes to local DC or old Cassandra versions
                    // (1.1 knows how to forward old-style String message IDs; updated to int in 2.0)
                    if (localDataCenter.equals(dc))
                    {
                        if (localDc == null)
                            localDc = new ArrayList<>(targets.selectedReplicas().size());

                        localDc.add(destination);
                    }
                    else
                    {
                        if (dcGroups == null)
                            dcGroups = new HashMap<>();

                        Collection<Replica> messages = dcGroups.get(dc);
                        if (messages == null)
                            messages = dcGroups.computeIfAbsent(dc, (v) -> new ArrayList<>(3)); // most DCs will have <= 3 replicas

                        messages.add(destination);
                    }

                    if (backPressureHosts == null)
                        backPressureHosts = new ArrayList<>(targets.selectedReplicas().size());

                    backPressureHosts.add(destination.endpoint());
                }
            }
            else
            {
                //Immediately mark the response as expired since the request will not be sent
                responseHandler.expired();
                if (shouldHint(destination))
                {
                    if (endpointsToHint == null)
                        endpointsToHint = new ArrayList<>();

                    endpointsToHint.add(destination);
                }
            }
        }

        if (backPressureHosts != null)
            MessagingService.instance().applyBackPressure(backPressureHosts, responseHandler.currentTimeout());

        if (endpointsToHint != null)
            submitHint(mutation, EndpointsForToken.copyOf(mutation.key().getToken(), endpointsToHint), responseHandler);

        if (insertLocal)
        {
            Preconditions.checkNotNull(localReplica);
            performLocally(stage, localReplica, Optional.of(mutation), mutation::apply, responseHandler);
        }

        if (localDc != null)
        {
            for (Replica destination : localDc)
                MessagingService.instance().sendWriteRR(message, destination, responseHandler, true);
        }
        if (dcGroups != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            for (Collection<Replica> dcTargets : dcGroups.values())
                sendMessagesToNonlocalDC(message, EndpointsForToken.copyOf(mutation.key().getToken(), dcTargets), responseHandler);
        }
    }

    private static void checkHintOverload(Replica destination)
    {
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        if (StorageMetrics.totalHintsInProgress.getCount() > maxHintsInProgress
                && (getHintsInProgressFor(destination.endpoint()).get() > 0 && shouldHint(destination)))
        {
            throw new OverloadedException("Too many in flight hints: " + StorageMetrics.totalHintsInProgress.getCount() +
                                          " destination: " + destination +
                                          " destination hints: " + getHintsInProgressFor(destination.endpoint()).get());
        }
    }

    private static void sendMessagesToNonlocalDC(MessageOut<? extends IMutation> message,
                                                 EndpointsForToken targets,
                                                 AbstractWriteResponseHandler<IMutation> handler)
    {
        Iterator<Replica> iter = targets.iterator();
        int[] messageIds = new int[targets.size()];
        Replica target = iter.next();

        int idIdx = 0;
        // Add the other destinations of the same message as a FORWARD_HEADER entry
        while (iter.hasNext())
        {
            Replica destination = iter.next();
            int id = MessagingService.instance().addWriteCallback(handler,
                                                                  message,
                                                                  destination,
                                                                  message.getTimeout(),
                                                                  handler.replicaPlan.consistencyLevel(),
                                                                  true);
            messageIds[idIdx++] = id;
            logger.trace("Adding FWD message to {}@{}", id, destination);
        }
        // TODO: test non-local DCs
        Replicas.checkFull(targets);
        message = message.withParameter(ParameterType.FORWARD_TO.FORWARD_TO, new ForwardToContainer(targets.endpoints(), messageIds));
        // send the combined message + forward headers
        int id = MessagingService.instance().sendWriteRR(message, target, handler, true);
        logger.trace("Sending message to {}@{}", id, target);
    }

    private static void performLocally(Stage stage, Replica localReplica, final Runnable runnable)
    {
        StageManager.getStage(stage).maybeExecuteImmediately(new LocalMutationRunnable(localReplica)
        {
            public void runMayThrow()
            {
                try
                {
                    runnable.run();
                }
                catch (Exception ex)
                {
                    logger.error("Failed to apply mutation locally : ", ex);
                }
            }

            @Override
            protected Verb verb()
            {
                return MessagingService.Verb.MUTATION;
            }
        });
    }

    private static void performLocally(Stage stage, Replica localReplica, Optional<IMutation> mutation, final Runnable runnable, final IAsyncCallbackWithFailure<?> handler)
    {
        StageManager.getStage(stage).maybeExecuteImmediately(new LocalMutationRunnable(localReplica, mutation)
        {
            public void runMayThrow()
            {
                try
                {
                    runnable.run();
                    handler.response(null);
                }
                catch (Exception ex)
                {
                    if (!(ex instanceof WriteTimeoutException))
                        logger.error("Failed to apply mutation locally : ", ex);
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
                }
            }

            @Override
            protected Verb verb()
            {
                return MessagingService.Verb.MUTATION;
            }
        });
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime) throws UnavailableException, OverloadedException
    {
        Replica replica = findSuitableReplica(cm.getKeyspaceName(), cm.key(), localDataCenter, cm.consistency());

        if (replica.isLocal())
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter, queryStartNanoTime);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String keyspaceName = cm.getKeyspaceName();
            Keyspace keyspace = Keyspace.open(keyspaceName);
            AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();
            Token tk = cm.key().getToken();

            ReplicaLayout.ForToken replicaLayout = ReplicaLayout.forWrite(keyspace, cm.consistency(), tk, FailureDetector.instance::isAlive);
            rs.getWriteResponseHandler(replicaLayout, null, WriteType.COUNTER, queryStartNanoTime).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            AbstractWriteResponseHandler<IMutation> responseHandler = new WriteResponseHandler<>(ReplicaLayout.forCounterWrite(keyspace, tk, replica),
                                                                                                 WriteType.COUNTER, queryStartNanoTime);

            Tracing.trace("Enqueuing counter update to {}", replica);
            MessagingService.instance().sendWriteRR(cm.makeMutationMessage(), replica, responseHandler, false);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static Replica findSuitableReplica(String keyspaceName, DecoratedKey key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        EndpointsForToken replicas = StorageService.instance.getLiveNaturalReplicasForToken(keyspace, key);

        // CASSANDRA-13043: filter out those endpoints not accepting clients yet, maybe because still bootstrapping
        replicas = replicas.filter(replica -> StorageService.instance.isRpcReady(replica.endpoint()));

        // TODO have a way to compute the consistency level
        if (replicas.isEmpty())
            throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

        List<Replica> localReplicas = new ArrayList<>(replicas.size());

        for (Replica replica : replicas)
            if (snitch.getDatacenter(replica).equals(localDataCenter))
                localReplicas.add(replica);

        if (localReplicas.isEmpty())
        {
            // If the consistency required is local then we should not involve other DCs
            if (cl.isDatacenterLocal())
                throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

            // No endpoint in local DC, pick the closest endpoint according to the snitch
            replicas = snitch.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
            return replicas.get(0);
        }

        return localReplicas.get(ThreadLocalRandom.current().nextInt(localReplicas.size()));
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static AbstractWriteResponseHandler<IMutation> applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, Runnable callback, long queryStartNanoTime)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer, callback, WriteType.COUNTER, queryStartNanoTime);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static AbstractWriteResponseHandler<IMutation> applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter, long queryStartNanoTime)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer, null, WriteType.COUNTER, queryStartNanoTime);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final ReplicaLayout.ForToken targets,
                                             final AbstractWriteResponseHandler<IMutation> responseHandler,
                                             final String localDataCenter)
    {
        return new DroppableRunnable(MessagingService.Verb.COUNTER_MUTATION)
        {
            @Override
            public void runMayThrow() throws OverloadedException, WriteTimeoutException
            {
                assert mutation instanceof CounterMutation;

                Mutation result = ((CounterMutation) mutation).applyCounterMutation();
                responseHandler.response(null);
                sendToHintedReplicas(result, targets.withoutLocal(), responseHandler, localDataCenter, Stage.COUNTER_MUTATION);
            }
        };
    }

    private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!SchemaConstants.isLocalSystemKeyspace(cmd.metadata().keyspace))
                return false;
        return true;
    }

    public static RowIterator readOne(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return readOne(command, consistencyLevel, null, queryStartNanoTime);
    }

    public static RowIterator readOne(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, ClientState state, long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return PartitionIterators.getOnlyElement(read(SinglePartitionReadCommand.Group.one(command), consistencyLevel, state, queryStartNanoTime), command);
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        // When using serial CL, the ClientState should be provided
        assert !consistencyLevel.isSerialConsistency();
        return read(group, consistencyLevel, null, queryStartNanoTime);
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, ClientState state, long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(group.queries))
        {
            readMetrics.unavailables.mark();
            readMetricsMap.get(consistencyLevel).unavailables.mark();
            throw new IsBootstrappingException();
        }

        return consistencyLevel.isSerialConsistency()
             ? readWithPaxos(group, consistencyLevel, state, queryStartNanoTime)
             : readRegular(group, consistencyLevel, queryStartNanoTime);
    }

    private static PartitionIterator readWithPaxos(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, ClientState state, long queryStartNanoTime)
    throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        assert state != null;
        if (group.queries.size() > 1)
            throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");

        long start = System.nanoTime();
        SinglePartitionReadCommand command = group.queries.get(0);
        TableMetadata metadata = command.metadata();
        DecoratedKey key = command.partitionKey();

        PartitionIterator result = null;
        try
        {
            // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
            ReplicaLayout.ForPaxos replicaLayout = ReplicaLayout.forPaxos(Keyspace.open(metadata.keyspace), key, consistencyLevel);

            // does the work of applying in-progress writes; throws UAE or timeout if it can't
            final ConsistencyLevel consistencyForCommitOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL
                                                                                   ? ConsistencyLevel.LOCAL_QUORUM
                                                                                   : ConsistencyLevel.QUORUM;

            try
            {
                final PaxosBallotAndContention pair = beginAndRepairPaxos(start, key, metadata, replicaLayout, consistencyLevel, consistencyForCommitOrFetch, false, state);
                if (pair.contentions > 0)
                    casReadMetrics.contention.update(pair.contentions);
            }
            catch (WriteTimeoutException e)
            {
                throw new ReadTimeoutException(consistencyLevel, 0, consistencyLevel.blockFor(Keyspace.open(metadata.keyspace)), false);
            }
            catch (WriteFailureException e)
            {
                throw new ReadFailureException(consistencyLevel, e.received, e.blockFor, false, e.failureReasonByEndpoint);
            }

            result = fetchRows(group.queries, consistencyForCommitOrFetch, queryStartNanoTime);
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            casReadMetrics.unavailables.mark();
            readMetricsMap.get(consistencyLevel).unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            casReadMetrics.timeouts.mark();
            readMetricsMap.get(consistencyLevel).timeouts.mark();
            throw e;
        }
        catch (ReadFailureException e)
        {
            readMetrics.failures.mark();
            casReadMetrics.failures.mark();
            readMetricsMap.get(consistencyLevel).failures.mark();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            readMetricsMap.get(consistencyLevel).addNano(latency);
            Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }

        return result;
    }

    @SuppressWarnings("resource")
    private static PartitionIterator readRegular(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        long start = System.nanoTime();
        try
        {
            PartitionIterator result = fetchRows(group.queries, consistencyLevel, queryStartNanoTime);
            // Note that the only difference between the command in a group must be the partition key on which
            // they applied.
            boolean enforceStrictLiveness = group.queries.get(0).metadata().enforceStrictLiveness();
            // If we have more than one command, then despite each read command honoring the limit, the total result
            // might not honor it and so we should enforce it
            if (group.queries.size() > 1)
                result = group.limits().filter(result, group.nowInSec(), group.selectsFullPartition(), enforceStrictLiveness);
            return result;
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            readMetricsMap.get(consistencyLevel).unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            readMetricsMap.get(consistencyLevel).timeouts.mark();
            throw e;
        }
        catch (ReadFailureException e)
        {
            readMetrics.failures.mark();
            readMetricsMap.get(consistencyLevel).failures.mark();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            readMetricsMap.get(consistencyLevel).addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : group.queries)
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }
    }

    private static PartitionIterator concatAndBlockOnRepair(List<PartitionIterator> iterators, List<ReadRepair> repairs)
    {
        PartitionIterator concatenated = PartitionIterators.concat(iterators);

        if (repairs.isEmpty())
            return concatenated;

        return new PartitionIterator()
        {
            public void close()
            {
                concatenated.close();
                repairs.forEach(ReadRepair::maybeSendAdditionalRepairs);
                repairs.forEach(ReadRepair::awaitRepairs);
            }

            public boolean hasNext()
            {
                return concatenated.hasNext();
            }

            public RowIterator next()
            {
                return concatenated.next();
            }
        };
    }

    /**
     * This function executes local and remote reads, and blocks for the results:
     *
     * 1. Get the replica locations, sorted by response time according to the snitch
     * 2. Send a data request to the closest replica, and digest requests to either
     *    a) all the replicas, if read repair is enabled
     *    b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     * 3. Wait for a response from R replicas
     * 4. If the digests (if any) match the data return the data
     * 5. else carry out read repair by getting data from all the nodes.
     */
    private static PartitionIterator fetchRows(List<SinglePartitionReadCommand> commands, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        int cmdCount = commands.size();

        AbstractReadExecutor[] reads = new AbstractReadExecutor[cmdCount];

        for (int i=0; i<cmdCount; i++)
        {
            reads[i] = AbstractReadExecutor.getReadExecutor(commands.get(i), consistencyLevel, queryStartNanoTime);
        }

        for (int i=0; i<cmdCount; i++)
        {
            reads[i].executeAsync();
        }

        for (int i=0; i<cmdCount; i++)
        {
            reads[i].maybeTryAdditionalReplicas();
        }

        for (int i=0; i<cmdCount; i++)
        {
            reads[i].awaitResponses();
        }

        for (int i=0; i<cmdCount; i++)
        {
            reads[i].maybeSendAdditionalDataRequests();
        }

        for (int i=0; i<cmdCount; i++)
        {
            reads[i].awaitReadRepair();
        }

        List<PartitionIterator> results = new ArrayList<>(cmdCount);
        List<ReadRepair> repairs = new ArrayList<>(cmdCount);
        for (int i=0; i<cmdCount; i++)
        {
            results.add(reads[i].getResult());
            repairs.add(reads[i].getReadRepair());
        }

        return concatAndBlockOnRepair(results, repairs);
    }

    public static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback handler;
        private final long start = System.nanoTime();

        public LocalReadRunnable(ReadCommand command, ReadCallback handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            try
            {
                command.setMonitoringTime(constructionTime, false, verb.getTimeout(), DatabaseDescriptor.getSlowQueryTimeout());

                ReadResponse response;
                try (ReadExecutionController executionController = command.executionController();
                     UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
                {
                    response = command.createResponse(iterator);
                }

                if (command.complete())
                {
                    handler.response(response);
                }
                else
                {
                    MessagingService.instance().incrementDroppedMessages(verb, System.currentTimeMillis() - constructionTime);
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
                }

                MessagingService.instance().addLatency(FBUtilities.getBroadcastAddressAndPort(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
            catch (Throwable t)
            {
                if (t instanceof TombstoneOverwhelmingException)
                {
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                    logger.error(t.getMessage());
                }
                else
                {
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
                    throw t;
                }
            }
        }
    }

    public static EndpointsForToken getLiveSortedReplicasForToken(Keyspace keyspace, RingPosition pos)
    {
        return getLiveSortedReplicas(keyspace, pos).forToken(pos.getToken());
    }

    public static EndpointsForRange getLiveSortedReplicas(Keyspace keyspace, RingPosition pos)
    {
        EndpointsForRange liveReplicas = StorageService.instance.getLiveNaturalReplicas(keyspace, pos);
        // Replica availability is considered by the query path
        Preconditions.checkState(liveReplicas.isEmpty() || liveReplicas.stream().anyMatch(Replica::isFull),
                                 "At least one full replica required for reads: " + liveReplicas);

        return DatabaseDescriptor.getEndpointSnitch().sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), liveReplicas);
    }

    /**
     * Estimate the number of result rows per range in the ring based on our local data.
     * <p>
     * This assumes that ranges are uniformly distributed across the cluster and
     * that the queried data is also uniformly distributed.
     */
    private static float estimateResultsPerRange(PartitionRangeReadCommand command, Keyspace keyspace)
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        Index index = command.getIndex(cfs);
        float maxExpectedResults = index == null
                                 ? command.limits().estimateTotalResults(cfs)
                                 : index.getEstimatedResultRows();

        // adjust maxExpectedResults by the number of tokens this node has and the replication factor for this ks
        return (maxExpectedResults / DatabaseDescriptor.getNumTokens()) / keyspace.getReplicationStrategy().getReplicationFactor().replicas;
    }

    private static class RangeIterator extends AbstractIterator<ReplicaLayout.ForRange>
    {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;
        private final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
        private final int rangeCount;

        public RangeIterator(PartitionRangeReadCommand command, Keyspace keyspace, ConsistencyLevel consistency)
        {
            this.keyspace = keyspace;
            this.consistency = consistency;

            List<? extends AbstractBounds<PartitionPosition>> l = keyspace.getReplicationStrategy() instanceof LocalStrategy
                                                          ? command.dataRange().keyRange().unwrap()
                                                          : getRestrictedRanges(command.dataRange().keyRange());
            this.ranges = l.iterator();
            this.rangeCount = l.size();
        }

        public int rangeCount()
        {
            return rangeCount;
        }

        protected ReplicaLayout.ForRange computeNext()
        {
            if (!ranges.hasNext())
                return endOfData();

            AbstractBounds<PartitionPosition> range = ranges.next();
            EndpointsForRange liveReplicas = getLiveSortedReplicas(keyspace, range.right);

            int blockFor = consistency.blockFor(keyspace);
            EndpointsForRange targetReplicas = consistency.filterForQuery(keyspace, liveReplicas);
            int minResponses = Math.min(targetReplicas.size(), blockFor);

            // Endpoitns for range here as well
            return ReplicaLayout.forRangeRead(keyspace, consistency, range,
                                              liveReplicas, targetReplicas.subList(0, minResponses));
        }
    }

    private static class RangeMerger extends AbstractIterator<ReplicaLayout.ForRange>
    {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;
        private final PeekingIterator<ReplicaLayout.ForRange> ranges;

        private RangeMerger(Iterator<ReplicaLayout.ForRange> iterator, Keyspace keyspace, ConsistencyLevel consistency)
        {
            this.keyspace = keyspace;
            this.consistency = consistency;
            this.ranges = Iterators.peekingIterator(iterator);
        }

        protected ReplicaLayout.ForRange computeNext()
        {
            if (!ranges.hasNext())
                return endOfData();

            ReplicaLayout.ForRange current = ranges.next();

            // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
            // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
            // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
            while (ranges.hasNext())
            {
                // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                // don't know how to deal with a wrapping range.
                // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                // wire compatibility, so It's likely easier not to bother;
                if (current.range.right.isMinimum())
                    break;

                ReplicaLayout.ForRange next = ranges.peek();

                EndpointsForRange merged = Replicas.keepEndpoints(current.natrualReplicas(), next.natrualReplicas().endpoints());

                // Check if there is enough endpoint for the merge to be possible.
                if (!consistency.isSufficientLiveNodes(keyspace, merged))
                    break;

                EndpointsForRange filteredMerged = consistency.filterForQuery(keyspace, merged);

                // Estimate whether merging will be a win or not
                // TODO: we should ideally also expand our EndpointsForRange
                // unfortunately these are currently only Endpoints<?>, due to conflation of behaviours in just a couple of places
                if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, current.selectedReplicas(), next.selectedReplicas()))
                    break;

                // If we get there, merge this range and the next one
                current = ReplicaLayout.forRangeRead(keyspace, consistency, current.range.withNewRight(next.range.right), merged, filteredMerged);
                ranges.next(); // consume the range we just merged since we've only peeked so far
            }
            return current;
        }
    }

    private static class SingleRangeResponse extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final DataResolver resolver;
        private final ReadCallback handler;
        private final ReadRepair readRepair;
        private PartitionIterator result;

        private SingleRangeResponse(DataResolver resolver, ReadCallback handler, ReadRepair readRepair)
        {
            this.resolver = resolver;
            this.handler = handler;
            this.readRepair = readRepair;
        }

        private void waitForResponse() throws ReadTimeoutException
        {
            if (result != null)
                return;

            handler.awaitResults();
            result = resolver.resolve();
        }

        protected RowIterator computeNext()
        {
            waitForResponse();
            return result.hasNext() ? result.next() : endOfData();
        }

        public void close()
        {
            if (result != null)
                result.close();
        }
    }

    private static class RangeCommandIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final Iterator<ReplicaLayout.ForRange> ranges;
        private final int totalRangeCount;
        private final PartitionRangeReadCommand command;
        private final boolean enforceStrictLiveness;

        private final long startTime;
        private final long queryStartNanoTime;
        private DataLimits.Counter counter;
        private PartitionIterator sentQueryIterator;

        private int concurrencyFactor;
        // The two following "metric" are maintained to improve the concurrencyFactor
        // when it was not good enough initially.
        private int liveReturned;
        private int rangesQueried;

        public RangeCommandIterator(RangeIterator ranges, PartitionRangeReadCommand command, int concurrencyFactor, Keyspace keyspace, ConsistencyLevel consistency, long queryStartNanoTime)
        {
            this.command = command;
            this.concurrencyFactor = concurrencyFactor;
            this.startTime = System.nanoTime();
            this.ranges = new RangeMerger(ranges, keyspace, consistency);
            this.totalRangeCount = ranges.rangeCount();
            this.queryStartNanoTime = queryStartNanoTime;
            this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
        }

        public RowIterator computeNext()
        {
            try
            {
                while (sentQueryIterator == null || !sentQueryIterator.hasNext())
                {
                    // If we don't have more range to handle, we're done
                    if (!ranges.hasNext())
                        return endOfData();

                    // else, sends the next batch of concurrent queries (after having close the previous iterator)
                    if (sentQueryIterator != null)
                    {
                        liveReturned += counter.counted();
                        sentQueryIterator.close();

                        // It's not the first batch of queries and we're not done, so we we can use what has been
                        // returned so far to improve our rows-per-range estimate and update the concurrency accordingly
                        updateConcurrencyFactor();
                    }
                    sentQueryIterator = sendNextRequests();
                }

                return sentQueryIterator.next();
            }
            catch (UnavailableException e)
            {
                rangeMetrics.unavailables.mark();
                throw e;
            }
            catch (ReadTimeoutException e)
            {
                rangeMetrics.timeouts.mark();
                throw e;
            }
            catch (ReadFailureException e)
            {
                rangeMetrics.failures.mark();
                throw e;
            }
        }

        private void updateConcurrencyFactor()
        {
            if (liveReturned == 0)
            {
                // we haven't actually gotten any results, so query all remaining ranges at once
                concurrencyFactor = totalRangeCount - rangesQueried;
                return;
            }

            // Otherwise, compute how many rows per range we got on average and pick a concurrency factor
            // that should allow us to fetch all remaining rows with the next batch of (concurrent) queries.
            int remainingRows = command.limits().count() - liveReturned;
            float rowsPerRange = (float)liveReturned / (float)rangesQueried;
            concurrencyFactor = Math.max(1, Math.min(totalRangeCount - rangesQueried, Math.round(remainingRows / rowsPerRange)));
            logger.trace("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                         rowsPerRange, remainingRows, concurrencyFactor);
        }

        /**
         * Queries the provided sub-range.
         *
         * @param plan the subRange to query.
         * @param isFirst in the case where multiple queries are sent in parallel, whether that's the first query on
         * that batch or not. The reason it matters is that whe paging queries, the command (more specifically the
         * {@code DataLimits}) may have "state" information and that state may only be valid for the first query (in
         * that it's the query that "continues" whatever we're previously queried).
         */
        private SingleRangeResponse query(ReplicaLayout.ForRange plan, boolean isFirst)
        {
            PartitionRangeReadCommand rangeCommand = command.forSubRange(plan.range, isFirst);
            // TODO: Maybe a concrete class for that case wouldn't hurt
            ReadRepair<EndpointsForRange, ReplicaLayout.ForRange> readRepair = new BlockingReadRepair<>(command, plan, queryStartNanoTime);
            DataResolver<EndpointsForRange, ReplicaLayout.ForRange> resolver = new DataResolver<>(rangeCommand, plan, readRepair, queryStartNanoTime);
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);

            ReadCallback<EndpointsForRange, ReplicaLayout.ForRange> handler = new ReadCallback<>(resolver,
                                                                              plan.consistencyLevel().blockFor(keyspace),
                                                                              rangeCommand,
                                                                              plan,
                                                                              queryStartNanoTime);

            handler.assureSufficientLiveNodes();
            if (plan.selectedReplicas().size() == 1 && plan.selectedReplicas().get(0).isLocal())
            {
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(rangeCommand, handler));
            }
            else
            {
                for (Replica replica : plan.selectedReplicas())
                {
                    Tracing.trace("Enqueuing request to {}", replica);
                    MessagingService.instance().sendRRWithFailure(rangeCommand.createMessage(), replica.endpoint(), handler);
                }
            }

            return new SingleRangeResponse(resolver, handler, readRepair);
        }

        private PartitionIterator sendNextRequests()
        {
            List<PartitionIterator> concurrentQueries = new ArrayList<>(concurrencyFactor);
            List<ReadRepair> readRepairs = new ArrayList<>(concurrencyFactor);
            for (int i = 0; i < concurrencyFactor && ranges.hasNext(); i++)
            {
                @SuppressWarnings("resource")
                SingleRangeResponse response = query(ranges.next(), i == 0);
                concurrentQueries.add(response);
                readRepairs.add(response.readRepair);
                ++rangesQueried;
            }

            Tracing.trace("Submitted {} concurrent range requests", concurrentQueries.size());
            // We want to count the results for the sake of updating the concurrency factor (see updateConcurrencyFactor) but we don't want to
            // enforce any particular limit at this point (this could break code than rely on postReconciliationProcessing), hence the DataLimits.NONE.
            counter = DataLimits.NONE.newCounter(command.nowInSec(), true, command.selectsFullPartition(), enforceStrictLiveness);
            return counter.applyTo(concatAndBlockOnRepair(concurrentQueries, readRepairs));
        }

        public void close()
        {
            try
            {
                if (sentQueryIterator != null)
                    sentQueryIterator.close();
            }
            finally
            {
                long latency = System.nanoTime() - startTime;
                rangeMetrics.addNano(latency);
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
            }
        }
    }

    @SuppressWarnings("resource")
    public static PartitionIterator getRangeSlice(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    {
        Tracing.trace("Computing ranges to query");

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        RangeIterator ranges = new RangeIterator(command, keyspace, consistencyLevel);

        // our estimate of how many result rows there will be per-range
        float resultsPerRange = estimateResultsPerRange(command, keyspace);
        // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
        // fetch enough rows in the first round
        resultsPerRange -= resultsPerRange * CONCURRENT_SUBREQUESTS_MARGIN;
        int concurrencyFactor = resultsPerRange == 0.0
                              ? 1
                              : Math.max(1, Math.min(ranges.rangeCount(), (int) Math.ceil(command.limits().count() / resultsPerRange)));
        logger.trace("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                     resultsPerRange, command.limits().count(), ranges.rangeCount(), concurrencyFactor);
        Tracing.trace("Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)", ranges.rangeCount(), concurrencyFactor, resultsPerRange);

        // Note that in general, a RangeCommandIterator will honor the command limit for each range, but will not enforce it globally.

        return command.limits().filter(command.postReconciliationProcessing(new RangeCommandIterator(ranges, command, concurrencyFactor, keyspace, consistencyLevel, queryStartNanoTime)),
                                       command.nowInSec(),
                                       command.selectsFullPartition(),
                                       command.metadata().enforceStrictLiveness());
    }

    public Map<String, List<String>> getSchemaVersions()
    {
        return describeSchemaVersions(false);
    }

    public Map<String, List<String>> getSchemaVersionsWithPort()
    {
        return describeSchemaVersions(true);
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions(boolean withPort)
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddressAndPort, UUID> versions = new ConcurrentHashMap<>();
        final Set<InetAddressAndPort> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        IAsyncCallback<UUID> cb = new IAsyncCallback<UUID>()
        {
            public void response(MessageIn<UUID> message)
            {
                // record the response from the remote node.
                versions.put(message.from, message.payload);
                latch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        // an empty message acts as a request to the SchemaVersionVerbHandler.
        MessageOut message = new MessageOut(MessagingService.Verb.SCHEMA_CHECK);
        for (InetAddressAndPort endpoint : liveHosts)
            MessagingService.instance().sendRR(message, endpoint, cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddressAndPort> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddressAndPort host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress(withPort));
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum())
        {
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<T>> ranges = new ArrayList<AbstractBounds<T>>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
        AbstractBounds<T> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
             *   - if remainder is tokens, then we'll just split using the provided token.
             *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
             *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
             *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
             *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
             *     keys having 15 as token and B include none of those (since that is what our node owns).
             * asSplitValue() abstracts that choice.
             */
            Token upperBoundToken = ringIter.next();
            T upperBound = (T)upperBoundToken.upperBound(queryRange.left.getClass());
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<T>,AbstractBounds<T>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);

        return ranges;
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        synchronized (StorageService.instance)
        {
            if (b)
                StorageService.instance.checkServiceAllowedToStart("hinted handoff");

            DatabaseDescriptor.setHintedHandoffEnabled(b);
        }
    }

    public void enableHintsForDC(String dc)
    {
        DatabaseDescriptor.enableHintsForDC(dc);
    }

    public void disableHintsForDC(String dc)
    {
        DatabaseDescriptor.disableHintsForDC(dc);
    }

    public Set<String> getHintedHandoffDisabledDCs()
    {
        return DatabaseDescriptor.hintedHandoffDisabledDCs();
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(Replica replica)
    {
        if (DatabaseDescriptor.hintedHandoffEnabled())
        {
            Set<String> disabledDCs = DatabaseDescriptor.hintedHandoffDisabledDCs();
            if (!disabledDCs.isEmpty())
            {
                final String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(replica);
                if (disabledDCs.contains(dc))
                {
                    Tracing.trace("Not hinting {} since its data center {} has been disabled {}", replica, dc, disabledDCs);
                    return false;
                }
            }
            boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(replica.endpoint()) > DatabaseDescriptor.getMaxHintWindow();
            if (hintWindowExpired)
            {
                HintsService.instance.metrics.incrPastWindow(replica.endpoint());
                Tracing.trace("Not hinting {} which has been down {} ms", replica, Gossiper.instance.getEndpointDowntime(replica.endpoint()));
            }
            return !hintWindowExpired;
        }
        else
        {
            return false;
        }
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down.
     * @throws TimeoutException
     */
    public static void truncateBlocking(String keyspace, String cfname) throws UnavailableException, TimeoutException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);
        if (isAnyStorageHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddressAndPort> allEndpoints = StorageService.instance.getLiveRingMembers(true);

        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        Tracing.trace("Enqueuing truncate messages to hosts {}", allEndpoints);
        final Truncation truncation = new Truncation(keyspace, cfname);
        MessageOut<Truncation> message = truncation.createMessage();
        for (InetAddressAndPort endpoint : allEndpoints)
            MessagingService.instance().sendRR(message, endpoint, responseHandler);

        // Wait for all
        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            Tracing.trace("Timed out");
            throw e;
        }
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyStorageHostDown()
    {
        return !Gossiper.instance.getUnreachableTokenOwners().isEmpty();
    }

    public interface WritePerformer
    {
        public void apply(IMutation mutation,
                          ReplicaLayout.ForToken targets,
                          AbstractWriteResponseHandler<IMutation> responseHandler,
                          String localDataCenter,
                          ConsistencyLevel consistencyLevel) throws OverloadedException;
    }

    /**
     * This class captures metrics for views writes.
     */
    private static class ViewWriteMetricsWrapped extends BatchlogResponseHandler<IMutation>
    {
        public ViewWriteMetricsWrapped(AbstractWriteResponseHandler<IMutation> writeHandler, int i, BatchlogCleanup cleanup, long queryStartNanoTime)
        {
            super(writeHandler, i, cleanup, queryStartNanoTime);
            viewWriteMetrics.viewReplicasAttempted.inc(totalEndpoints());
        }

        public void response(MessageIn<IMutation> msg)
        {
            super.response(msg);
            viewWriteMetrics.viewReplicasSuccess.inc();
        }
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        final long constructionTime;
        final MessagingService.Verb verb;

        public DroppableRunnable(MessagingService.Verb verb)
        {
            this.constructionTime = System.currentTimeMillis();
            this.verb = verb;
        }

        public final void run()
        {
            long timeTaken = System.currentTimeMillis() - constructionTime;
            if (timeTaken > verb.getTimeout())
            {
                MessagingService.instance().incrementDroppedMessages(verb, timeTaken);
                return;
            }
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();

        private final Replica localReplica;
        private final Optional<IMutation> mutationOpt;

        public LocalMutationRunnable(Replica localReplica, Optional<IMutation> mutationOpt)
        {
            this.localReplica = localReplica;
            this.mutationOpt = mutationOpt;
        }

        public LocalMutationRunnable(Replica localReplica)
        {
            this.localReplica = localReplica;
            this.mutationOpt = Optional.empty();
        }

        public final void run()
        {
            final MessagingService.Verb verb = verb();
            long mutationTimeout = verb.getTimeout();
            long timeTaken = System.currentTimeMillis() - constructionTime;
            if (timeTaken > mutationTimeout)
            {
                if (MessagingService.DROPPABLE_VERBS.contains(verb))
                    MessagingService.instance().incrementDroppedMutations(mutationOpt, timeTaken);

                HintRunnable runnable = new HintRunnable(EndpointsForToken.of(localReplica.range().right, localReplica))
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected MessagingService.Verb verb();
        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final EndpointsForToken targets;

        protected HintRunnable(EndpointsForToken targets)
        {
            this.targets = targets;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                StorageMetrics.totalHintsInProgress.dec(targets.size());
                for (InetAddressAndPort target : targets.endpoints())
                    getHintsInProgressFor(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return StorageMetrics.totalHints.getCount();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return (int) StorageMetrics.totalHintsInProgress.getCount();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    private static AtomicInteger getHintsInProgressFor(InetAddressAndPort destination)
    {
        try
        {
            return hintsInProgress.load(destination);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static Future<Void> submitHint(Mutation mutation, Replica target, AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        return submitHint(mutation, EndpointsForToken.of(target.range().right, target), responseHandler);
    }

    public static Future<Void> submitHint(Mutation mutation,
                                          EndpointsForToken targets,
                                          AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        // TODO: test hints
        Replicas.checkFull(targets);
        HintRunnable runnable = new HintRunnable(targets)
        {
            public void runMayThrow()
            {
                Set<InetAddressAndPort> validTargets = new HashSet<>(targets.size());
                Set<UUID> hostIds = new HashSet<>(targets.size());
                for (InetAddressAndPort target : targets.endpoints())
                {
                    UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
                    if (hostId != null)
                    {
                        hostIds.add(hostId);
                        validTargets.add(target);
                    }
                    else
                        logger.debug("Discarding hint for endpoint not part of ring: {}", target);
                }
                logger.trace("Adding hints for {}", validTargets);
                HintsService.instance.write(hostIds, Hint.create(mutation, System.currentTimeMillis()));
                validTargets.forEach(HintsService.instance.metrics::incrCreatedHints);
                // Notify the handler only for CL == ANY
                if (responseHandler != null && responseHandler.replicaPlan.consistencyLevel() == ConsistencyLevel.ANY)
                    responseHandler.response(null);
            }
        };

        return submitHint(runnable);
    }

    private static Future<Void> submitHint(HintRunnable runnable)
    {
        StorageMetrics.totalHintsInProgress.inc(runnable.targets.size());
        for (Replica target : runnable.targets)
            getHintsInProgressFor(target.endpoint()).incrementAndGet();
        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCounterWriteRpcTimeout() { return DatabaseDescriptor.getCounterWriteRpcTimeout(); }
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis); }

    public Long getCasContentionTimeout() { return DatabaseDescriptor.getCasContentionTimeout(); }
    public void setCasContentionTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }

    public Long getNativeTransportMaxConcurrentConnections() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnections(); }
    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnections(nativeTransportMaxConcurrentConnections); }

    public Long getNativeTransportMaxConcurrentConnectionsPerIp() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp(); }
    public void setNativeTransportMaxConcurrentConnectionsPerIp(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnectionsPerIp(nativeTransportMaxConcurrentConnections); }

    public void reloadTriggerClasses() { TriggerExecutor.instance.reloadClasses(); }

    public long getReadRepairAttempted()
    {
        return ReadRepairMetrics.attempted.getCount();
    }

    public long getReadRepairRepairedBlocking()
    {
        return ReadRepairMetrics.repairedBlocking.getCount();
    }

    public long getReadRepairRepairedBackground()
    {
        return ReadRepairMetrics.repairedBackground.getCount();
    }

    public int getNumberOfTables()
    {
        return Schema.instance.getNumberOfTables();
    }

    public String getIdealConsistencyLevel()
    {
        return DatabaseDescriptor.getIdealConsistencyLevel().toString();
    }

    public String setIdealConsistencyLevel(String cl)
    {
        ConsistencyLevel original = DatabaseDescriptor.getIdealConsistencyLevel();
        ConsistencyLevel newCL = ConsistencyLevel.valueOf(cl.trim().toUpperCase());
        DatabaseDescriptor.setIdealConsistencyLevel(newCL);
        return String.format("Updating ideal consistency level new value: %s old value %s", newCL, original.toString());
    }

    @Override
    public void configureFullQueryLogger(String path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize)
    {
        path = path != null ? path : DatabaseDescriptor.getFullQueryLogPath();
        Preconditions.checkNotNull(path, "cassandra.yaml did not set full_query_log_dir and not set as parameter");
        AuditLogManager.getInstance().configureFQL(Paths.get(path), rollCycle, blocking, maxQueueWeight, maxLogSize);
    }

    @Override
    public void resetFullQueryLogger()
    {
        AuditLogManager.getInstance().resetFQL(DatabaseDescriptor.getFullQueryLogPath());
    }

    @Override
    public void stopFullQueryLogger()
    {
        AuditLogManager.getInstance().disableFQL();
    }

    public int getOtcBacklogExpirationInterval() {
        return DatabaseDescriptor.getOtcBacklogExpirationInterval();
    }

    public void setOtcBacklogExpirationInterval(int intervalInMillis) {
        DatabaseDescriptor.setOtcBacklogExpirationInterval(intervalInMillis);
    }

//
//    // TODO: migrate to layout
//    static class PaxosParticipants
//    {
//        final EndpointsForToken liveReplicas;
//        final int participants;
//
//        PaxosParticipants(EndpointsForToken liveReplicas, int participants)
//        {
//            this.liveReplicas = liveReplicas;
//            this.participants = participants;
//        }
//
//        @Override
//        public final int hashCode()
//        {
//            int hashCode = 31 + (liveReplicas == null ? 0 : liveReplicas.hashCode());
//            return 31 * hashCode * this.participants;
//        }
//
//        @Override
//        public final boolean equals(Object o)
//        {
//            if(!(o instanceof PaxosParticipants))
//                return false;
//            PaxosParticipants that = (PaxosParticipants)o;
//            // handles nulls properly
//            return Objects.equals(liveReplicas, that.liveReplicas) && participants == that.participants;
//        }
//    }

    static class PaxosBallotAndContention
    {
        final UUID ballot;
        final int contentions;

        PaxosBallotAndContention(UUID ballot, int contentions)
        {
            this.ballot = ballot;
            this.contentions = contentions;
        }

        @Override
        public final int hashCode()
        {
            int hashCode = 31 + (ballot == null ? 0 : ballot.hashCode());
            return 31 * hashCode * this.contentions;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof PaxosBallotAndContention))
                return false;
            PaxosBallotAndContention that = (PaxosBallotAndContention)o;
            // handles nulls properly
            return Objects.equals(ballot, that.ballot) && contentions == that.contentions;
        }
    }
}
