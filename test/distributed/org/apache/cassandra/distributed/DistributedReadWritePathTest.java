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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.net.MessagingService.Verb.READ_REPAIR;

public class DistributedReadWritePathTest extends DistributedTestBase
{
    @Test
    public void repairTest() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            for (int i = 1; i <= 2; i++)
            {
                for (int j = 0; j < 100; j++)
                {
                    cluster.get(i).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)", j * i, j);
                    cluster.get(i).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (2, ?, ?)", j * i, j);
                    cluster.get(i).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", 100 + i, j, j);
                }
            }

            cluster.get(1).repair(KEYSPACE, new HashMap<String, String>() {{ }});
            Thread.sleep(10000);

            Object[][] res1 = cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl");
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl"),
                       res1);
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl"),
                       res1);
            System.out.println("res1.length = " + res1.length);

        }
    }

    @Test
    public void coordinatorRead() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                     ConsistencyLevel.ALL,
                                                     1),
                       row(1, 1, 1),
                       row(1, 2, 2),
                       row(1, 3, 3));
        }
    }

    @Test
    public void coordinatorWrite() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                          ConsistencyLevel.QUORUM);

            for (int i = 0; i < 3; i++)
            {
                assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                           row(1, 1, 1));
            }

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.QUORUM),
                       row(1, 1, 1));
        }
    }

    @Test
    public void readRepairTest() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Verify that data got repaired to the third node
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }

    @Test
    public void failingReadRepairTest() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            cluster.verbs(READ_REPAIR).to(3).drop();
            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Data was not repaired
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));
        }
    }

    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

            // Introduce schema disagreement
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

            Exception thrown = null;
            try
            {
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                              ConsistencyLevel.QUORUM);
            }
            catch (RuntimeException e)
            {
                thrown = e;
            }

            Assert.assertTrue(thrown.getMessage().contains("Exception occurred on the node"));
            Assert.assertTrue(thrown.getCause().getMessage().contains("Unknown column v2 during deserialization"));
        }
    }

    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

            // Introduce schema disagreement
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

            Exception thrown = null;
            try
            {
                assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                         ConsistencyLevel.ALL),
                           row(1, 1, 1, null));
            }
            catch (Exception e)
            {
                thrown = e;
            }
            Assert.assertTrue(thrown.getMessage().contains("Exception occurred on the node"));
            Assert.assertTrue(thrown.getCause().getMessage().contains("Unknown column v2 during deserialization"));
        }
    }

    @Test
    public void reAddColumnAsStatic() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            for (int i = 1; i <= 3; i++)
            {
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, ?, ?)",
                                              ConsistencyLevel.ALL,
                                              1, i, i);
            }

            // Drop column
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl DROP v1");

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.ALL),
                       row(1, 1),
                       row(1, 2),
                       row(1, 3));

            // Drop column
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v1 int static");

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.ALL),
                       row(1, 1, null),
                       row(1, 2, null),
                       row(1, 3, null));

            cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v1) VALUES (?, ?)",
                                          ConsistencyLevel.ALL,
                                          1, 1);

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.ALL),
                       row(1, 1, 1),
                       row(1, 2, 1),
                       row(1, 3, 1));
        }
    }

    @Test
    public void reAddColumnAsStaticDisagreementCoordinatorSide() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            for (int i = 1; i <= 3; i++)
            {
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, ?, ?)",
                                              ConsistencyLevel.ALL,
                                              1, i, i);
            }

            // Drop column
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl DROP v1", 1);

            Exception thrown = null;
            try
            {
                cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                              ConsistencyLevel.ALL);
            }
            catch (Exception e)
            {
                thrown = e;
            }

            Assert.assertTrue(thrown.getCause().getMessage().contains("[v1] is not a subset of"));

            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v1 int static", 1);

            try
            {
                cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                              ConsistencyLevel.ALL);
            }
            catch (Exception e)
            {
                thrown = e;
            }

            Assert.assertTrue(thrown.getCause().getMessage().contains("[v1] is not a subset of"));
        }
    }

    @Test
    public void reAddColumnAsStaticDisagreementReplicaSide() throws Throwable
    {
        try (TestCluster cluster = createCluster(2))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            for (int i = 1; i <= 3; i++)
            {
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, ?, ?)",
                                              ConsistencyLevel.ALL,
                                              1, i, i);
            }

            // Drop column on the replica
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl DROP v1", 2);

            // Columns are going to be read and read-repaired as long as they're available
            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.ALL),
                       row(1, 1, 1),
                       row(1, 2, 2),
                       row(1, 3, 3));

            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1),
                       row(1, 2),
                       row(1, 3));

            // Re-add as static on the replica
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v1 int static", 2);

            // Try reading
            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.ALL),
                       row(1, 1, 1),
                       row(1, 2, 2),
                       row(1, 3, 3));

            // Make sure read-repair did not corrupt the data
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, null),
                       row(1, 2, null),
                       row(1, 3, null));

            // Writing to the replica with disagreeing schema should not work
            Exception thrown = null;
            try
            {
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, ?, ?)",
                                              ConsistencyLevel.ALL,
                                              1, 1, 5);
            }
            catch (Exception e)
            {
                thrown = e;
            }

            Assert.assertNotNull(thrown);

            thrown = null;

            // If somehow replica got new data, reading that data should not be possible, either
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, ?, ?)",
                                           1, 1, 100);

            try
            {
                assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                         ConsistencyLevel.ALL),
                           row(1, 1, 1),
                           row(1, 2, 2),
                           row(1, 3, 3));
            }
            catch (Exception e)
            {
                thrown = e;
            }

            Assert.assertNotNull(thrown);
        }
    }

    @Test
    public void monotonicPagingTest() throws Throwable
    {
        try (TestCluster cluster = createCluster(1))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))");

            for (int i = 1; i <= 5; i++)
            {
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, s, v) VALUES (1, ?, ?, ?)",
                                              ConsistencyLevel.QUORUM,
                                              i, i, i);
            }

            Iterator<Object[]> iter = cluster.coordinator().executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                                              ConsistencyLevel.QUORUM,
                                                                              2);

            int staticValue = (int) iter.next()[2];

            cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, s, v) VALUES (1, ?, ?, ?)",
                                          ConsistencyLevel.QUORUM,
                                          100, 100, 100);

            while (iter.hasNext())
            {
                Object[] row = iter.next();
                Assert.assertEquals(staticValue, row[2]);
            }
        }
    }
}
