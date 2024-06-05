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

package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.util.EnumMap;
import java.util.IdentityHashMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.NoOpListener;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.ClusterActions.Options;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Semaphore;

import static org.apache.cassandra.simulator.cluster.ClusterActions.InitialConfiguration.initializeAll;
import static org.apache.cassandra.simulator.cluster.ClusterActions.Options.noActions;

public class TrivialSimulationTest extends SimulationTestBase
{
    @Test
    public void trivialTest() throws IOException // for demonstration/experiment purposes
    {
        simulate((simulation) -> {
                     Options options = noActions(simulation.cluster.size());
                     ClusterActions clusterActions = new ClusterActions(simulation.simulated, simulation.cluster,
                                                                        options, new NoOpListener(), new Debug(new EnumMap<>(Debug.Info.class), new int[0]));
                     return ActionList.of(clusterActions.initializeCluster(initializeAll(simulation.cluster.size())),
                                          simulation.schemaChange(1, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}"),
                                          simulation.schemaChange(1, "CREATE TABLE IF NOT EXISTS ks.tbl (pk int PRIMARY KEY, v int)"));
                 },
                 (simulation) -> ActionList.of(simulation.executeQuery(1, "INSERT INTO ks.tbl VALUES (1,1)", ConsistencyLevel.QUORUM),
                                               simulation.executeQuery(1, "SELECT * FROM ks.tbl WHERE pk = 1", ConsistencyLevel.QUORUM)),
                 (simulation) -> ActionList.of(),
                 (config) -> config
                             .threadCount(10)
                             .nodes(3, 3)
                             .dcs(1, 1));
    }

    @Test
    public void componentTest()
    {
        simulate(arr(() -> {
                     ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("name", 10);
                     CountDownLatch latch = CountDownLatch.newCountDownLatch(5);

                     for (int i = 0; i < 5; i++)
                     {
                         executor.submit(() -> {
                             latch.decrement();
                             try
                             {
                                 latch.await();
                             }
                             catch (InterruptedException e)
                             {
                                 throw new RuntimeException(e);
                             }
                         });
                     }
                 }),
                 () -> {});
    }

    @Test
    public void semaphoreTest()
    {
        simulate(arr(() -> {
                     ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("semaphore-test-", 10);
                     Semaphore semaphore = Semaphore.newSemaphore(5);
                     CountDownLatch latch = CountDownLatch.newCountDownLatch(5);

                     for (int i = 0; i < 5; i++)
                     {
                         executor.submit(() -> {
                             for (int j = 0; j < 100; j++)
                             {
                                 int permits = semaphore.permits();
                                 Assert.assertTrue(permits + " should be non negative", permits >= 0);

                                 try
                                 {
                                     semaphore.acquire(1);
                                     semaphore.release(1);
                                 }
                                 catch (Throwable e)
                                 {
                                     throw new RuntimeException(e);
                                 }
                             }
                             latch.decrement();
                         });
                     }

                     latch.awaitUninterruptibly();
                     int permits = semaphore.permits();
                     Assert.assertEquals(5, permits);
                 }),
                 () -> {});
    }

    @Test
    public void identityHashMapTest()
    {
        simulate(arr(() -> new IdentityHashMap<>().put(1, 1)),
                 () -> {});
    }
}
