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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.ITestCluster;
import org.apache.cassandra.distributed.api.InstanceVersion;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.IInvokableInstance.invokable;

/**
 * TestCluster creates, initializes and manages Cassandra instances ({@link Instance}.
 *
 * All instances created under the same cluster will have a shared ClassLoader that'll preload
 * common classes required for configuration and communication (byte buffers, primitives, config
 * objects etc). Shared classes are listed in {@link InstanceClassLoader#sharedClassNames}.
 *
 * Each instance has its own class loader that will load logging, yaml libraries and all non-shared
 * Cassandra package classes. The rule of thumb is that we'd like to have all Cassandra-specific things
 * (unless explitily shared through the common classloader) on a per-classloader basis in order to
 * allow creating more than one instance of DatabaseDescriptor and other Cassandra singletones.
 *
 * All actions (reading, writing, schema changes, etc) are executed by serializing lambda/runnables,
 * transferring them to instance-specific classloaders, deserializing and running them there. Most of
 * the things can be simply captured in closure or passed through `apply` method of the wrapped serializable
 * function/callable. You can use {@link InvokableInstance#{applies|runs|consumes}OnInstance} for executing
 * code on specific instance.
 *
 * Each instance has its own logger. Each instance log line will contain INSTANCE{instance_id}.
 *
 * As of today, messaging is faked by hooking into MessagingService, so we're not using usual Cassandra
 * handlers for internode to have more control over it. Messaging is wired by passing verbs manually.
 * coordinator-handling code and hooks to the callbacks can be found in {@link Coordinator}.
 */
public class TestCluster implements ITestCluster, AutoCloseable
{
    private static ExecutorService exec = Executors.newCachedThreadPool(new NamedThreadFactory("cluster-async-tasks"));

    private class InstanceWrapper extends DelegatingInstance
    {
        private volatile boolean isShutdown = true;
        private volatile InstanceVersion version;
        private final IInstanceConfig config;

        private InstanceWrapper(InstanceVersion version, IInstanceConfig config)
        {
            super(null);
            this.config = config;
            setVersion(version);
        }

        @Override
        public void startup()
        {
            delegate.startup(TestCluster.this);
            isShutdown = false;
        }

        @Override
        public void shutdown()
        {
            isShutdown = true;
            delegate.shutdown();
        }

        @Override
        public void receiveMessage(Message message)
        {
            if (!isShutdown) // since we invoke directly on the other node, we drop messages immediately if we are shutdown
                delegate.receiveMessage(message);
        }

        @Override
        public void setVersion(InstanceVersion version)
        {
            if (!isShutdown)
                throw new IllegalStateException("Must be shutdown before version can be modified");
            // re-initialise
            this.version = version;
            ClassLoader classLoader = new InstanceClassLoader(config.num(), version.getClassPath(), sharedClassLoader);
            delegate = new InvokableInstance(classLoader)
                    .appliesOnInstance(Instance::new)
                    .apply(config, classLoader);
        }
    }

    private final File root;
    // immutable
    private final ClassLoader sharedClassLoader;

    // mutated by starting/stopping a node
    private final List<InstanceWrapper> instances;
    private final Map<InetAddressAndPort, InstanceWrapper> instanceMap;

    // mutated by user-facing API
    private final MessageFilters filters;

    private TestCluster(File root, List<InstanceVersion> versions, List<IInstanceConfig> configs, ClassLoader sharedClassLoader)
    {
        this.root = root;
        this.sharedClassLoader = sharedClassLoader;
        this.instances = new ArrayList<>();
        this.instanceMap = new HashMap<>();
        for (int i = 0 ; i < versions.size() ; ++i)
        {
            InstanceWrapper wrapper = new InstanceWrapper(versions.get(i), configs.get(i));
            instances.add(wrapper);
            instanceMap.put(configs.get(i).broadcastAddress(), wrapper);
        }
        this.filters = new MessageFilters(this);
    }

    public ICoordinator coordinator() { return coordinator(1); }
    /**
     * WARNING: we index from 1 here, for consistency with inet address!
     */
    public ICoordinator coordinator(int node) { return instances.get(node - 1).callOnInstance(Coordinator::new); }
    /**
     * WARNING: we index from 1 here, for consistency with inet address!
     */
    public IInstance get(int node) { return instances.get(node - 1); }
    public IInstance get(InetAddressAndPort addr) { return instanceMap.get(addr); }

    public int size()
    {
        return instances.size();
    }
    public Stream<IInstance> stream() { return instances.stream().map(IInstance.class::cast); }
    public void forEach(IInvokableInstance.SerializableRunnable runnable) { forEach(invokable(runnable)); }
    public void forEach(Consumer<? super IInstance> consumer) { instances.forEach(consumer); }
    public void parallelForEach(IInvokableInstance.SerializableRunnable runnable) { parallelForEach(invokable(runnable)); }
    public void parallelForEach(Consumer<? super IInstance> consumer)
    {
        FBUtilities.waitOnFutures(instances.stream()
                .map(i -> exec.submit(() -> consumer.accept(i)))
                .collect(Collectors.toList())
        );
    }


    public IMessageFilters filters() { return filters; }
    MessageFilters.Builder verbs(MessagingService.Verb ... verbs) { return filters.verbs(verbs); }

    public void disableAutoCompaction(String keyspace)
    {
        forEach(() -> {
            for (ColumnFamilyStore cs : Keyspace.open(keyspace).getColumnFamilyStores())
                cs.disableAutoCompaction();
        });
    }

    public void schemaChange(String query)
    {
        try (SchemaChangeMonitor monitor = new SchemaChangeMonitor())
        {
            // execute the schema change
            coordinator().execute(query, ConsistencyLevel.ALL);
            monitor.waitForAgreement();
        }
    }

    /**
     * Will wait for a schema change AND agreement that occurs after it is created
     * (and precedes the invocation to waitForAgreement)
     *
     * Works by simply checking if all UUIDs agree after any schema version change event,
     * so long as the waitForAgreement method has been entered (indicating the change has
     * taken place on the coordinator)
     *
     * This could perhaps be made a little more robust, but this should more than suffice.
     */
    public class SchemaChangeMonitor implements AutoCloseable
    {
        public SchemaChangeMonitor() {}

        @Override
        public void close() { }

        public void waitForAgreement()
        {
            long start = System.nanoTime();
            while (1 != instances.stream().map(IInstance::getSchemaVersion).distinct().count())
            {
                if (System.nanoTime() - start > TimeUnit.MINUTES.toNanos(1L))
                    throw new IllegalStateException("Schema agreement not reached");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        }
    }

    public void schemaChange(String statement, int instance)
    {
        get(instance).schemaChange(statement);
    }

    private void startup()
    {
        parallelForEach(IInstance::startup);
    }

    public static TestCluster create(int nodeCount) throws Throwable
    {
        return create(nodeCount, Files.createTempDirectory("dtests").toFile());
    }
    public static TestCluster create(int nodeCount, File root)
    {
        return create(InstanceVersion.current(nodeCount), root);
    }

    public static TestCluster create(List<InstanceVersion> versions) throws IOException
    {
        return create(versions, Files.createTempDirectory("dtests").toFile());
    }
    public static TestCluster create(List<InstanceVersion> versions, File root)
    {
        root.mkdirs();
        setupLogging(root);

        ClassLoader sharedClassLoader = Thread.currentThread().getContextClassLoader();

        List<IInstanceConfig> configs = new ArrayList<>();
        long token = Long.MIN_VALUE + 1, increment = 2 * (Long.MAX_VALUE / versions.size());
        for (int i = 0 ; i < versions.size() ; ++i)
        {
            InstanceConfig config = InstanceConfig.generate(i + 1, root, String.valueOf(token));
            configs.add(config);
            token += increment;
        }

        TestCluster cluster = new TestCluster(root, versions, configs, sharedClassLoader);
        cluster.startup();
        return cluster;
    }

    private static void setupLogging(File root)
    {
        try
        {
            String testConfPath = "test/conf/logback-dtest.xml";
            Path logConfPath = Paths.get(root.getPath(), "/logback-dtest.xml");
            if (!logConfPath.toFile().exists())
            {
                Files.copy(new File(testConfPath).toPath(),
                           logConfPath);
            }
            System.setProperty("logback.configurationFile", "file://" + logConfPath);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        List<Future<?>> futures = instances.stream()
                .map(i -> exec.submit(i::shutdown))
                .collect(Collectors.toList());

//        withThreadLeakCheck(futures);

        // Make sure to only delete directory when threads are stopped
        exec.submit(() -> {
            FBUtilities.waitOnFutures(futures);
            FileUtils.deleteRecursive(root);
        });
    }

    // We do not want this check to run every time until we fix problems with tread stops
    private void withThreadLeakCheck(List<Future<?>> futures)
    {
        FBUtilities.waitOnFutures(futures);

        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        threadSet = Sets.difference(threadSet, Collections.singletonMap(Thread.currentThread(), null).keySet());
        if (!threadSet.isEmpty())
        {
            for (Thread thread : threadSet)
            {
                System.out.println(thread);
                System.out.println(Arrays.toString(thread.getStackTrace()));
            }
            throw new RuntimeException(String.format("Not all threads have shut down. %d threads are still running: %s", threadSet.size(), threadSet));
        }
    }

}

