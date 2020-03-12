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

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.DistributedTestSnitch;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;

public class MessageFiltersTest extends TestBaseImpl
{
    @Test
    public void testFilters() throws Throwable
    {
        String read = "SELECT * FROM " + KEYSPACE + ".tbl";
        String write = "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)";

        try (ICluster cluster = builder().withNodes(2).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Reads and writes are going to time out in both directions
            cluster.filters().allVerbs().from(1).to(2).drop();
            for (int i : new int[]{ 1, 2 })
                assertTimeOut(() -> cluster.coordinator(i).execute(read, ConsistencyLevel.ALL));
            for (int i : new int[]{ 1, 2 })
                assertTimeOut(() -> cluster.coordinator(i).execute(write, ConsistencyLevel.ALL));

            cluster.filters().reset();
            // Reads are going to timeout only when 1 serves as a coordinator
            cluster.filters().verbs(Verb.RANGE_REQ.id).from(1).to(2).drop();
            assertTimeOut(() -> cluster.coordinator(1).execute(read, ConsistencyLevel.ALL));
            cluster.coordinator(2).execute(read, ConsistencyLevel.ALL);

            // Writes work in both directions
            for (int i : new int[]{ 1, 2 })
                cluster.coordinator(i).execute(write, ConsistencyLevel.ALL);
        }
    }

    @Test
    public void testMessageMatching() throws Throwable
    {
        String read = "SELECT * FROM " + KEYSPACE + ".tbl";
        String write = "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)";

        try (ICluster<IInvokableInstance> cluster = builder().withNodes(2).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            AtomicInteger counter = new AtomicInteger();

            Set<Integer> verbs = Sets.newHashSet(Arrays.asList(Verb.RANGE_REQ.id,
                                                               Verb.RANGE_RSP.id,
                                                               Verb.MUTATION_REQ.id,
                                                               Verb.MUTATION_RSP.id));

            for (boolean inbound : Arrays.asList(true, false))
            {
                counter.set(0);
                // Reads and writes are going to time out in both directions
                IMessageFilters.Filter filter = cluster.filters()
                                                       .allVerbs()
                                                       .inbound(inbound)
                                                       .from(1)
                                                       .to(2)
                                                       .messagesMatching((from, to, msg) -> {
                                                           // Decode and verify message on instance; return the result back here
                                                           Integer id = cluster.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<Integer>) () -> {
                                                               Message decoded = Instance.deserializeMessage(msg);
                                                               return (Integer) decoded.verb().id;
                                                           }).call();
                                                           Assert.assertTrue(verbs.contains(id));
                                                           counter.incrementAndGet();
                                                           return false;
                                                       }).drop();

                for (int i : new int[]{ 1, 2 })
                    cluster.coordinator(i).execute(read, ConsistencyLevel.ALL);
                for (int i : new int[]{ 1, 2 })
                    cluster.coordinator(i).execute(write, ConsistencyLevel.ALL);

                filter.off();
                Assert.assertEquals(4, counter.get());
            }
        }
    }

    @Test
    public void outboundBeforeInbound() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = Cluster.create(2))
        {
            NetworkTopology.AddressAndPort other = cluster.get(2).broadcastAddress();
            CountDownLatch waitForIt = new CountDownLatch(1);
            Set<Integer> outboundMessagesSeen = new HashSet<>();
            Set<Integer> inboundMessagesSeen = new HashSet<>();
            AtomicBoolean outboundAfterInbound = new AtomicBoolean(false);
            cluster.filters().outbound().verbs(Verb.ECHO_REQ.id, Verb.ECHO_RSP.id).messagesMatching((from, to, msg) -> {
                outboundMessagesSeen.add(msg.verb());
                if (inboundMessagesSeen.contains(msg.verb()))
                    outboundAfterInbound.set(true);
                return false;
            }).drop(); // drop is confusing since I am not dropping, im just listening...
            cluster.filters().inbound().verbs(Verb.ECHO_REQ.id, Verb.ECHO_RSP.id).messagesMatching((from, to, msg) -> {
                inboundMessagesSeen.add(msg.verb());
                return false;
            }).drop(); // drop is confusing since I am not dropping, im just listening...
            cluster.filters().inbound().verbs(Verb.ECHO_RSP.id).messagesMatching((from, to, msg) -> {
                waitForIt.countDown();
                return false;
            }).drop(); // drop is confusing since I am not dropping, im just listening...
            cluster.get(1).runOnInstance(() -> {
                MessagingService.instance().send(Message.out(Verb.ECHO_REQ, NoPayload.noPayload),
                                                 InetAddressAndPort.getByAddressOverrideDefaults(other.getAddress(), other.getPort()));
            });

            waitForIt.await();

            Assert.assertEquals(outboundMessagesSeen, inboundMessagesSeen);
            // since both are equal, only need to confirm the size of one
            Assert.assertEquals(2, outboundMessagesSeen.size());
            Assert.assertFalse("outbound message saw after inbound", outboundAfterInbound.get());
        }
    }

    private static void assertTimeOut(Runnable r)
    {
        try
        {
            r.run();
            Assert.fail("Should have timed out");
        }
        catch (Throwable t)
        {
            if (!t.toString().contains("TimeoutException"))
                throw t;
            // ignore
        }
    }
}