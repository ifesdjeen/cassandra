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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class PagingTest extends TestBaseImpl
{
    @Test
    public void testReverseIteration() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck1 int, regular int, PRIMARY KEY (pk, ck1)) WITH CLUSTERING ORDER BY (ck1 DESC)");
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck1, regular) values (1,1,1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck1, regular) values (1,2,2)", ConsistencyLevel.ALL);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            Iterator<Object[]> iter = cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl " +
                                                                               "WHERE pk=1 AND ck1<=1 ORDER BY ck1 ASC;",
                                                                               ConsistencyLevel.QUORUM, 1);
            assertRows(iter,
                       row(1, 1, 1));
        }

    }

    @Test
    public void testPagingWithRangeTombstones() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, regular int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 1 AND ck < 10", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,1,1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,2,2)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,3,3)", ConsistencyLevel.ALL);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            Iterator<Object[]> iter = cluster.coordinator(1).executeWithPaging("SELECT pk,ck,regular FROM " + KEYSPACE + ".tbl " +
                                                                               "WHERE pk=? AND ck>=? ORDER BY ck DESC;",
                                                                               ConsistencyLevel.QUORUM, 1,
                                                                               1, 1);

            assertRows(iter,
                       row(1, 3, 3),
                       row(1, 2, 2),
                       row(1, 1, 1));
        }
    }
}
