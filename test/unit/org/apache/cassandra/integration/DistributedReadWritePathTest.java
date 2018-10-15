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

import org.junit.Test;

import static org.apache.cassandra.integration.TestCluster.KEYSPACE;

public class DistributedReadWritePathTest extends DistributedTestBase
{
    @Test
    public void coordinatorRead() throws Throwable
    {
        try (TestCluster cluster = TestCluster.create(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.get(0).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

            assertRows(cluster.coordinatorRead("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1),
                       row(1, 2, 2),
                       row(1, 3, 3));
        }
    }

    @Test
    public void coordinatorWrite() throws Throwable
    {
        try (TestCluster cluster = TestCluster.create(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.coordinatorWrite("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            for (int i = 0; i < 3; i++)
            {
                assertRows(cluster.get(0).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                           row(1, 1, 1));
            }

            assertRows(cluster.coordinatorRead("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }
    @Test
    public void readRepairTest() throws Throwable
    {
        try (TestCluster cluster = TestCluster.create(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.get(0).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.coordinatorRead("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }
}
