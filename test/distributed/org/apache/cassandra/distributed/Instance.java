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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.ITestCluster;
import org.apache.cassandra.distributed.api.InstanceVersion;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPool;

public class Instance extends InvokableInstance implements IInstance
{
    public final IInstanceConfig config;

    public Instance(IInstanceConfig config, ClassLoader classLoader)
    {
        super(classLoader);
        this.config = config;
    }

    public IInstanceConfig config()
    {
        return config;
    }

    public InetAddressAndPort getBroadcastAddress() { return LegacyAdapter.getBroadcastAddressAndPort(); }

    public Object[][] executeInternal(String query, Object... args)
    {
        ParsedStatement.Prepared prepared = QueryProcessor.prepareInternal(query);
        ResultMessage result = prepared.statement.executeInternal(QueryProcessor.internalQueryState(),
                QueryProcessor.makeInternalOptions(prepared, args));

        if (result instanceof ResultMessage.Rows)
            return RowUtil.toObjects((ResultMessage.Rows)result);
        else
            return null;
    }

    public UUID getSchemaVersion()
    {
        // we do not use method reference syntax here, because we need to invoke on the node-local schema instance
        //noinspection Convert2MethodRef
        return Schema.instance.getVersion();
    }

    public void schemaChange(String query)
    {
        try
        {
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(SystemKeyspace.NAME);
            QueryState queryState = new QueryState(state);

            CQLStatement statement = QueryProcessor.parseStatement(query, queryState).statement;
            statement.validate(state);

            QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
            statement.executeInternal(queryState, options);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
        }
    }

    private void registerMockMessaging(ITestCluster cluster)
    {
        BiConsumer<InetAddressAndPort, Message> deliverToInstance = (to, message) -> cluster.get(to).receiveMessage(message);
        BiConsumer<InetAddressAndPort, Message> deliverToInstanceIfNotFiltered = cluster.filters().filter(deliverToInstance);

        Map<InetAddress, InetAddressAndPort> addressAndPortMap = new HashMap<>();
        cluster.stream().map(IInstance::getBroadcastAddress).forEach(addressAndPort -> {
            if (null != addressAndPortMap.put(addressAndPort.address, addressAndPort))
                throw new IllegalStateException("This version of Cassandra does not support multiple nodes with the same InetAddress");
        });

        MessagingService.instance().addMessageSink(
                new MessageDeliverySink(deliverToInstanceIfNotFiltered, addressAndPortMap::get));
    }

    private static class MessageDeliverySink implements IMessageSink
    {
        private final BiConsumer<InetAddressAndPort, Message> deliver;
        private final Function<InetAddress, InetAddressAndPort> lookupAddressAndPort;
        MessageDeliverySink(BiConsumer<InetAddressAndPort, Message> deliver, Function<InetAddress, InetAddressAndPort> lookupAddressAndPort)
        {
            this.deliver = deliver;
            this.lookupAddressAndPort = lookupAddressAndPort;
        }

        public boolean allowOutgoingMessage(MessageOut messageOut, int id, InetAddress to)
        {
            try (DataOutputBuffer out = new DataOutputBuffer(1024))
            {
                InetAddressAndPort from = LegacyAdapter.getBroadcastAddressAndPort();
                InetAddressAndPort toFull = lookupAddressAndPort.apply(to);
                messageOut.serialize(out, MessagingService.current_version);
                deliver.accept(toFull, new Message(messageOut.verb.ordinal(), out.toByteArray(), id, MessagingService.current_version, from));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            // we can filter to our heart's content on the outgoing message; no need to worry about incoming
            return true;
        }
    }

    public void receiveMessage(Message message)
    {
        try (DataInputBuffer in = new DataInputBuffer(message.bytes()))
        {
            MessageIn<?> messageIn = MessageIn.read(in, message.version(), message.id());
            Runnable deliver = new MessageDeliveryTask(messageIn, message.id(), System.currentTimeMillis(), false);
            deliver.run();
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Exception occurred on the node " + LegacyAdapter.getBroadcastAddressAndPort(), t);
        }
    }

    public void startup(ITestCluster cluster)
    {
        try
        {
            mkdirs();
            InstanceIDDefiner.instanceId = config.num();

            Config.setOverrideLoadConfig(() -> loadConfig(config));
            DatabaseDescriptor.setDaemonInitialized();
            DatabaseDescriptor.createAllDirectories();
            Keyspace.setInitialized();
            SystemKeyspace.persistLocalMetadata();
            initializeRing(cluster);
            registerMockMessaging(cluster);
        }
        catch (Throwable t)
        {
            if (t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new RuntimeException(t);
        }
    }

    private void mkdirs()
    {
        new File(config.getString("saved_caches_directory")).mkdirs();
        new File(config.getString("hints_directory")).mkdirs();
        new File(config.getString("commitlog_directory")).mkdirs();
        for (String dir : (String[]) config.get("data_file_directories"))
            new File(dir).mkdirs();
    }

    public static Config loadConfig(IInstanceConfig overrides)
    {
        Config config = new Config();
        overrides.propagate(config);
        return config;
    }

    private void initializeRing(ITestCluster cluster)
    {
        // This should be done outside instance in order to avoid serializing config
        String partitionerName = config.getString("partitioner");
        List<String> initialTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();
        for (int i = 1 ; i <= cluster.size() ; ++i)
        {
            IInstanceConfig config = cluster.get(i).config();
            initialTokens.add(config.getString("initial_token"));
            hosts.add(config.broadcastAddress());
            hostIds.add(config.hostId());
        }

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
                Gossiper.instance.initializeNodeUnsafe(ep.address, hostIds.get(i), 1);
                Gossiper.instance.injectApplicationState(ep.address,
                        ApplicationState.TOKENS,
                        new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(tokens.get(i))));
                storageService.onChange(ep.address,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(tokens.get(i))));
                Gossiper.instance.realMarkAlive(ep.address, Gossiper.instance.getEndpointStateForEndpoint(ep.address));
                MessagingService.instance().setVersion(ep.address, MessagingService.current_version);
            }

            // check that all nodes are in token metadata
            for (int i = 0; i < tokens.size(); ++i)
                assert storageService.getTokenMetadata().isMember(hosts.get(i).address);
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public void shutdown()
    {
        Throwable error = null;
        error = runAndMergeThrowable(error,
                BatchlogManager.instance::shutdown,
                HintsService.instance::shutdownBlocking,
                CommitLog.instance::shutdownBlocking,
                CompactionManager.instance::forceShutdown,
                Gossiper.instance::stop,
                SecondaryIndexManager::shutdownExecutors,
                MessagingService.instance()::shutdown,
                ColumnFamilyStore::shutdownFlushExecutor,
                ColumnFamilyStore::shutdownPostFlushExecutor,
                ColumnFamilyStore::shutdownReclaimExecutor,
                PendingRangeCalculatorService.instance::shutdownExecutor,
                BufferPool::shutdownLocalCleaner,
                Ref::shutdownReferenceReaper,
                StorageService.instance::shutdownBGMonitor,
                StageManager::shutdownAndWait,
                SharedExecutorPool.SHARED::shutdown,
                Memtable.MEMORY_POOL::shutdown,
                ScheduledExecutors::shutdownAndWait);
        Throwables.maybeFail(error);
    }

    @Override
    public void setVersion(InstanceVersion version)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startup()
    {
        throw new UnsupportedOperationException();
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

    private static Throwable runAndMergeThrowable(Throwable existing, ThrowingRunnable ... runnables)
    {
        for (ThrowingRunnable runnable : runnables)
        {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                existing = Throwables.merge(existing, t);
            }
        }
        return existing;
    }

    public static interface ThrowingRunnable
    {
        public void run() throws Throwable;
    }
}
