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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.schema.Schema;

public class AlterTest extends TestBaseImpl
{
    @Test
    public void testAtomicityOfSchemaUpdates() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1)
                                                                  .start()))
        {
            List<Thread> threads = new ArrayList<>();
            AtomicInteger counter = new AtomicInteger();
            for (int i = 0; i < 10; i++)
            {
                Thread thread = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted())
                    {
                        try
                        {
                            int cnt = counter.incrementAndGet();
                            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl" + cnt + " (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
                            cluster.get(1).runOnInstance(() -> {
                                assert Schema.instance.getKeyspaceInstance("distributed_test_keyspace").getColumnFamilyStore("tbl" + cnt) != null;
                            });
                        }
                        catch (Throwable t)
                        {
                            t.printStackTrace();
                        }
                    }

                });
                threads.add(thread);
                thread.start();
            }

            Thread.sleep(5000);
            for (Thread thread : threads)
            {
                thread.interrupt();
            }

            for (Thread thread : threads)
            {
                thread.join();
            }
            System.out.println("count = " + counter.get());
            Thread.sleep(10_000);
            cluster.get(1).runOnInstance(() -> {
                System.out.println("count = " + Keyspace.open("distributed_test_keyspace").getColumnFamilyStores().size());
            });
        }
    }

    @Test
    public void getAndSetCompressionParametersTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(2)
                                                                  .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
            cluster.stream().forEach((i) -> {
                i.acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<String>) (ks) -> {
                    Keyspace.open(ks)
                            .getColumnFamilyStore("tbl")
                            .setCompressionParametersJson("{\"chunk_length_in_kb\": \"128\"," +
                                                          "  \"class\": \"org.apache.cassandra.io.compress.LZ4Compressor\"}");
                    Assert.assertTrue(Keyspace.open(ks)
                                              .getColumnFamilyStore("tbl")
                                              .getCompressionParametersJson().contains("128"));
                }).accept(KEYSPACE);
            });
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int");

            cluster.stream().forEach((i) -> {
                i.acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<String>) (ks) -> {
                    Assert.assertFalse(Keyspace.open(ks)
                                               .getColumnFamilyStore("tbl")
                                               .getCompressionParametersJson().contains("128"));
                }).accept(KEYSPACE);
            });
        }
    }
}