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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
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

import com.google.common.io.Files;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TestCluster implements AutoCloseable
{
    static String KEYSPACE = "distributed_test_keyspace";

    // App class loader, holding classpath URLs
    private static final URLClassLoader contextClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();

    static Class<?>[] commonClasses = new Class[]
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

    static Set<String> classNames = new HashSet<>();

    private final File root;
    private final List<Instance> instances;
    private final Map<InetAddressAndPort, Instance> instanceMap;

    private TestCluster(File root, List<Instance> instances) throws Throwable
    {
        this.root = root;
        this.instances = instances;
        this.instanceMap = new HashMap<>();
        for (Instance instance : instances)
            instanceMap.put(instance.getBroadcastAddress(), instance);
    }

    public Instance get(int idx)
    {
        return instances.get(idx);
    }

    public Instance get(InetAddressAndPort addr)
    {
        return instanceMap.get(addr);
    }

    public Instance coordinator()
    {
        return get(0);
    }

    public Object[][] coordinatorRead(String statement)
    {
        return coordinator().coordinatorRead(String.format(statement, KEYSPACE),
                                             appliesOnInstance(Instance::handleRead));

    }

    public static TestCluster create(int nodeCount) throws Throwable
    {
        TestCluster cluster = create(nodeCount, Files.createTempDir());
        for (int i = 0; i < nodeCount; i++)
        {
            Instance instance = cluster.get(i);
            instance.initializeRing();
        }
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
        ExecutorService exec = Executors.newCachedThreadPool(new NamedThreadFactory("launch"));
        List<Future<?>> waitFor = new ArrayList<>(instances.size());
        for (Instance instance : instances)
            waitFor.add(exec.submit(instance::launch));
        exec.shutdown();
        FBUtilities.waitOnFutures(waitFor);
        return new TestCluster(root, instances);
    }

    @Override
    public void close()
    {
//        Set<Thread> threadSet1 = Thread.getAllStackTraces().keySet();
        for (Instance instance : instances)
            instance.shutdown();

        FileUtils.deleteRecursive(root);
//        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
//
//        threadSet = Sets.difference(threadSet, Collections.singletonMap(Thread.currentThread(), null).keySet());
//        if (!threadSet.isEmpty())
//        {
//            for (Thread thread : threadSet)
//            {
//                System.out.println(thread);
//                System.out.println(Arrays.toString(thread.getStackTrace()));
//            }
//            throw new RuntimeException("Not all threads have shut down: " + threadSet);
//        }
    }

    /**
     * Initializes a class loader shared between the classes
     */
    private static ClassLoader initializeCommonClassLoader() throws ClassNotFoundException
    {
        for (Class<?> k : commonClasses)
        {
            classNames.add(k.getName());
        }

        // Extension class loader
        ClassLoader rootClassLoader = contextClassLoader.getParent();

        URL[] urls = contextClassLoader.getURLs();
        ClassLoader cl = new URLClassLoader(urls, rootClassLoader);
        for (Class<?> k : commonClasses)
            cl.loadClass(k.getName());

        return contextClassLoader;
    }

    public static ClassLoader getInstanceClassLoader(ClassLoader commonClassLoader)
    {
        URL[] urls = contextClassLoader.getURLs();
        return new Instance.InstanceClassLoader(urls, classNames::contains, commonClassLoader);
    }

    public <I, O> BiFunction<InetAddressAndPort, I, O> appliesOnInstance(BiFunction<Instance, I, O> f)
    {
        return (endpoint, input) -> f.apply(get(endpoint), input);
    }

}

