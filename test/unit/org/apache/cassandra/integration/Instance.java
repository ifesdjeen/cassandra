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

package org.apache.cassandra.integration;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.junit.Assert.assertTrue;

public class Instance extends InvokableInstance
{
    final List<InstanceConfig> instanceConfigs;
    public Instance(File root, InstanceConfig config, List<InstanceConfig> instanceConfigs, ClassLoader classLoader)
    {
        super(root, config, classLoader);
        this.instanceConfigs = instanceConfigs;
    }

    public void initializeCassandraInstance(String configUrl)
    {
        runOnInstance(() ->
        {
            DatabaseDescriptor.daemonInitialization(() ->
            {
                try
                {
                    return new YamlConfigurationLoader().loadConfig(new URL(configUrl));
                }
                catch (MalformedURLException e)
                {
                    throw new RuntimeException(e);
                }
            });

            DatabaseDescriptor.createAllDirectories();
            Keyspace.setInitialized();
            SystemKeyspace.persistLocalMetadata();
        });
    }

    public void schemaChange(String query, UUID sharedId)
    {
        runOnInstance(() ->
        {
            try
            {
                ClientState state = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
                QueryState queryState = new QueryState(state);

                CQLStatement statement = QueryProcessor.parseStatement(query, queryState.getClientState());
                // Schema is propagated in a manual fashion, so table id won't be set by the coordinator.
                // In order to work around that, we have to set table id manually to be able to query the
                // same table from different nodes.
                if (statement instanceof CreateTableStatement)
                {
                    ((CreateTableStatement) statement).setTableId(sharedId.toString());
                }
                statement.validate(state);

                QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
                statement.executeLocally(queryState, options);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
            }
        });
    }

    public UntypedResultSet executeInternal(String query, Object... args)
    {
        return callOnInstance(() ->
        {
            QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeLocally(QueryProcessor.internalQueryState(),
                    QueryProcessor.makeInternalOptions(prepared.statement, args));

            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        });
    }

    public void initializeRing() throws UnknownHostException
    {
        // This should be done outside instance in order to avoid serializing config
        String partitionerName = config.partitioner;
        List<String> initialTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();
        for (InstanceConfig instanceConfig : instanceConfigs)
        {
            initialTokens.add(instanceConfig.initial_token);
            hosts.add(InetAddressAndPort.getByName(instanceConfig.broadcast_address));
            hostIds.add(instanceConfig.hostId);
        }

        runOnInstance(() ->
        {
            try
            {
                IPartitioner partitioner = FBUtilities.newPartitioner(partitionerName);
                StorageService storageService = StorageService.instance;
                List<Token> tokens = new ArrayList<>();
                for (String token : initialTokens)
                    tokens.add(partitioner.getTokenFactory().fromString(token));

                for (int i = 0; i < tokens.size(); i++)
                {
                    InetAddressAndPort ep = hosts.get(i);
                    Gossiper.instance.initializeNodeUnsafe(ep, hostIds.get(i), 1);
                    Gossiper.instance.injectApplicationState(ep,
                                                             ApplicationState.TOKENS,
                                                             new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(tokens.get(i))));
                    storageService.onChange(ep,
                                            ApplicationState.STATUS_WITH_PORT,
                                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(tokens.get(i))));
                    storageService.onChange(ep,
                                            ApplicationState.STATUS,
                                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(tokens.get(i))));
                    hosts.add(ep);
                }

                // check that all nodes are in token metadata
                for (int i = 0; i < tokens.size(); ++i)
                    assertTrue(storageService.getTokenMetadata().isMember(hosts.get(i)));
            }
            catch (Throwable e) // UnknownHostException
            {
                throw new RuntimeException(e);
            }
        });
    }

    public InetAddressAndPort getBroadcastAddress() { return callOnInstance(FBUtilities::getBroadcastAddressAndPort); }

    private static Object[][] doCoordinatorRead(String query, BiFunction<InetAddressAndPort, ByteBuffer, ByteBuffer> response)
    {
        // TODO: internal calls are not correct here
        CQLStatement prepared = QueryProcessor.getStatement(query, ClientState.forInternalCalls());
        assert prepared instanceof SelectStatement;
        SelectStatement selectStatement = (SelectStatement) prepared;
        ReadQuery readQuery = selectStatement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());

        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
            {
                try
                {
                    DataOutputBuffer buf = new DataOutputBuffer(1024);
                    message.serialize(buf, MessagingService.current_version);

                    ByteBuffer responseBB = response.apply(to, buf.buffer());

                    DataInputBuffer dib = new DataInputBuffer(responseBB, false);
                    ReadResponse readResponse = ReadResponse.serializer.deserialize(dib, MessagingService.current_version);

                    MessagingService.instance().receive(
                            new MessageIn<>(
                                    to, readResponse, Collections.emptyMap(),
                                    MessagingService.Verb.REQUEST_RESPONSE,
                                    MessagingService.current_version,
                                    System.nanoTime()
                            ), id
                    );
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return true;
            }
        });

        PartitionIterator pi = readQuery.execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), System.nanoTime());
        ResultMessage.Rows rows = selectStatement.processResults(pi, QueryOptions.DEFAULT, FBUtilities.nowInSeconds(), 10);
        Object[][] result = new Object[rows.result.rows.size()][];
        List<ColumnSpecification> specs = rows.result.metadata.names;
        for (int i = 0; i < rows.result.rows.size(); i++)
        {
            List<ByteBuffer> row = rows.result.rows.get(i);
            result[i] = new Object[row.size()];
            for (int j = 0; j < row.size(); j++)
            {
                result[i][j] = specs.get(j).type.getSerializer().deserialize(row.get(j));
            }
        }
        return result;
    }

    public Object[][] coordinatorRead(String query, BiFunction<InetAddressAndPort, ByteBuffer, ByteBuffer> responseHandler)
    {
        return appliesOnInstance(Instance::doCoordinatorRead).apply(query, responseHandler);
    }

    public ByteBuffer handleRead(ByteBuffer bb)
    {
        return appliesOnInstance((SerializableFunction<ByteBuffer, ByteBuffer>) in ->
        {
            DataOutputBuffer buf;
            try
            {
                DataInputBuffer dib = new DataInputBuffer(in, false);
                // TODO: refactor to use MessagingService / MessageIn
                dib.skipBytes(16);
                dib.readUnsignedVInt();

                ReadCommand cmd = ReadCommand.serializer.deserialize(dib, MessagingService.current_version);
                ReadResponse response;
                try (ReadExecutionController executionController = cmd.executionController();
                     UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
                {
                    response = cmd.createResponse(iterator);
                }

                // TODO: deliver MessageOut instead of just a response
                buf = new DataOutputBuffer(1024 * 8);
                ReadResponse.serializer.serialize(response, buf, MessagingService.current_version);
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return buf.buffer();
        }).apply(bb);
    }

    private void mkdirs()
    {
        new File(config.saved_caches_directory).mkdirs();
        new File(config.hints_directory).mkdirs();
        new File(config.commitlog_directory).mkdirs();
        for (String dir : config.data_file_directories)
            new File(dir).mkdirs();
    }

    void launch()
    {
        mkdirs();
        try
        {
            ConfigUtil.writeConfigFile(new File(root, "node" + config.num + "/cassandra.conf"), ConfigUtil.generateConfig(config));
            initializeCassandraInstance("file://" + root + "/node" + config.num + "/cassandra.conf");
        }
        catch (Throwable t)
        {
            if (t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new RuntimeException(t);
        }
    }

    public void shutdown()
    {
        runOnInstance(() -> {
            Throwable error = null;
            final CountDownLatch latch = new CountDownLatch(1);
            ScheduledExecutors.nonPeriodicTasks.execute(latch::countDown);
            error = runAndMergeThrowable(error, () -> latch.await(2, TimeUnit.SECONDS));
            error = runAndMergeThrowable(error, BatchlogManager.instance::shutdown);
            error = runAndMergeThrowable(error, HintsService.instance::shutdownBlocking);
            error = runAndMergeThrowable(error, CommitLog.instance::shutdownBlocking);
            error = runAndMergeThrowable(error, CompactionManager.instance::forceShutdown);
            error = runAndMergeThrowable(error, Gossiper.instance::stop);
            error = runAndMergeThrowable(error, SecondaryIndexManager::shutdownExecutors);
            error = runAndMergeThrowable(error, MessagingService.instance()::shutdown);
            error = shutdownAndWait(error, ActiveRepairService.repairCommandExecutor);
            error = runAndMergeThrowable(error, ColumnFamilyStore::shutdownFlushExecutor);
            error = runAndMergeThrowable(error, ColumnFamilyStore::shutdownPostFlushExecutor);
            error = runAndMergeThrowable(error, ColumnFamilyStore::shutdownReclaimExecutor);
            error = runAndMergeThrowable(error, ColumnFamilyStore::shutdownPerDiskFlushExecutors);
            error = shutdownAndWait(error, ScheduledExecutors.scheduledTasks);
            error = shutdownAndWait(error, ScheduledExecutors.optionalTasks);
            error = shutdownAndWait(error, ScheduledExecutors.scheduledFastTasks);
            error = shutdownAndWait(error, ScheduledExecutors.nonPeriodicTasks);
            error = runAndMergeThrowable(error, PendingRangeCalculatorService.instance::shutdownExecutor);
            error = runAndMergeThrowable(error, BufferPool::shutdownLocalCleaner);
            error = runAndMergeThrowable(error, Ref::shutdownReferenceReaper);

            // PENDING SEP Patch
//            for (ExecutorService stage : StageManager.stages.values())
//                error = shutdownAndWait(error, stage);
//            for (ExecutorService executor : SharedExecutorPool.SHARED.executors)
//                error = shutdownAndWait(error, executor);

            Memtable.MEMORY_POOL.getCleaner().interrupt();
            Memtable.MEMORY_POOL.getCleaner().trigger();

            Throwables.maybeFail(error);
        });
    }

    private static Throwable shutdownAndWait(Throwable existing, ExecutorService executor)
    {
        return runAndMergeThrowable(existing, () -> {
            executor.shutdown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            assert executor.isTerminated() && executor.isShutdown() : executor;
        });
    }

    private static Throwable runAndMergeThrowable(Throwable existing, ThrowingRunnable runnable)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable t)
        {
            return Throwables.merge(existing, t);
        }

        return existing;
    }

    public static interface ThrowingRunnable
    {
        public void run() throws Throwable;
    }
}
