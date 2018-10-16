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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TestCluster implements AutoCloseable
{
    static String KEYSPACE = "distributed_test_keyspace";
    private static ExecutorService exec = Executors.newCachedThreadPool(new NamedThreadFactory("cluster-async-tasks"));

    // App class loader, holding classpath URLs
    private static final URLClassLoader contextClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();

    // Classes that have to be shared between instances, for configuration or returning values
    static Class<?>[] sharedClasses = new Class[]
            {
                Pair.class,
                InstanceConfig.class,
                InetAddressAndPort.class,
                InvokableInstance.SerializableCallable.class,
                InvokableInstance.SerializableRunnable.class,
                InvokableInstance.SerializableConsumer.class,
                InvokableInstance.SerializableBiConsumer.class,
                InvokableInstance.SerializableFunction.class,
                InvokableInstance.SerializableBiFunction.class,
                InvokableInstance.InstanceFunction.class
            };

    static Set<String> sharedClassNames = new HashSet<String>();

    private final File root;
    private final List<Instance> instances;
    private final Coordinator coordinator;
    private final Map<InetAddressAndPort, Instance> instanceMap;

    private TestCluster(File root, List<Instance> instances) throws Throwable
    {
        this.root = root;
        this.instances = instances;
        this.instanceMap = new HashMap<>();
        for (Instance instance : instances)
            instanceMap.put(instance.getBroadcastAddress(), instance);
        this.coordinator = new Coordinator(this, instances.get(0));
    }

    public Instance get(int idx)
    {
        return instances.get(idx);
    }

    public Instance get(InetAddressAndPort addr)
    {
        return instanceMap.get(addr);
    }

    public Coordinator coordinator()
    {
        return coordinator;
    }

    public Object[][] coordinatorWrite(String statement, ConsistencyLevel consistencyLevel)
    {
        return coordinator().coordinatorWrite(String.format(statement, KEYSPACE), consistencyLevel.code);

    }

    public Object[][] coordinatorRead(String statement, ConsistencyLevel consistencyLevel)
    {
        return coordinator().coordinatorRead(String.format(statement, KEYSPACE), consistencyLevel.code);
    }

    public static TestCluster create(int nodeCount) throws Throwable
    {
        TestCluster cluster = create(nodeCount, Files.createTempDirectory("dtests").toFile());
        for (int i = 0; i < nodeCount; i++)
        {
            Instance instance = cluster.get(i);
            instance.initializeRing();
        }
        cluster.coordinator.init();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        return cluster;
    }

    public void schemaChange(String statement) throws Throwable
    {
        UUID sharedId = UUID.randomUUID();
        for (Instance instance : instances)
            instance.schemaChange(statement, sharedId);
    }

    public static TestCluster create(int nodeCount, File root) throws Throwable
    {
        root.mkdirs();
        setupLogging(root);

        ClassLoader common = initializeCommonClassLoader();
        List<Instance> instances = new ArrayList<>();
        long token = Long.MIN_VALUE + 1, increment = 2 * (Long.MAX_VALUE / nodeCount);
        List<InstanceConfig> instanceConfigs = new ArrayList<>();
        for (int i = 0 ; i < nodeCount ; ++i)
        {
            InstanceConfig instanceConfig = InstanceConfig.generate(i + 1, root, String.valueOf(token));
            instances.add(new Instance(root, instanceConfig, instanceConfigs, getInstanceClassLoader(common)));
            instanceConfigs.add(instanceConfig);
            token += increment;
        }

        FBUtilities.waitOnFutures(runOnAll(instances, Instance::launch, exec));
        return new TestCluster(root, instances);
    }

    private static void setupLogging(File root)
    {
        try
        {
            String testConfPath = "test/conf/logback-dtest.xml";
            Path logConfPath = Paths.get(root.getPath(), "/logback-dtest.xml");
            if (!logConfPath.toFile().exists())
            {
                System.out.println("logConfPath.toFile().exists() = " + logConfPath.toFile().exists());
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

    public List<Future<?>> runOnAll(Consumer<Instance> action)
    {
        return runOnAll(instances, action, exec);
    }

    public static List<Future<?>> runOnAll(List<Instance> instances, Consumer<Instance> action, ExecutorService exec)
    {
        Consumer<Integer> byIdx = runOnInstance(instances, action);
        List<Future<?>> futures = new ArrayList<>();
        for (Instance instance : instances)
        {
            int idx = instance.config.num - 1;
            Future<?> f = exec.submit(() -> instance.consumesOnInstance((Consumer<Integer> captured) -> captured.accept(idx)).accept(byIdx));
            futures.add(f);
        }
        return futures;
    }

    @Override
    public void close()
    {
        List<Future<?>> futures = runOnAll(Instance::shutdown);
//        withThreadLeakCheck(futures);
        FileUtils.deleteRecursive(root);

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
    /**
     * Initializes a class loader shared between the classes
     */
    private static ClassLoader initializeCommonClassLoader() throws ClassNotFoundException
    {
        for (Class<?> k : sharedClasses)
        {
            sharedClassNames.add(k.getName());
        }

        // Extension class loader
        ClassLoader rootClassLoader = contextClassLoader.getParent();

        URL[] urls = contextClassLoader.getURLs();
        ClassLoader cl = new URLClassLoader(urls, rootClassLoader);
        for (Class<?> k : sharedClasses)
            cl.loadClass(k.getName());

        return contextClassLoader;
    }

    public static ClassLoader getInstanceClassLoader(ClassLoader commonClassLoader)
    {
        URL[] urls = contextClassLoader.getURLs();
        return new Instance.InstanceClassLoader(urls, sharedClassNames::contains, commonClassLoader);
    }

    public <I, O> BiFunction<InetAddressAndPort, I, O> appliesOnInstance(BiFunction<Instance, I, O> f)
    {
        return (endpoint, input) -> f.apply(get(endpoint), input);
    }

    public static Consumer<Integer> runOnInstance(List<Instance> instances, Consumer<Instance> f)
    {
        return (idx) -> f.accept(instances.get(idx));
    }
}

