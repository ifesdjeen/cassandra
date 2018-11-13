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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;

public class DistributedTestBase
{
    static String KEYSPACE = "distributed_test_keyspace";

    @BeforeClass
    public static void setup()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    protected TestCluster createCluster(int subnet, int nodeCount) throws Throwable
    {
        TestCluster cluster = TestCluster.create(subnet, nodeCount);
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + nodeCount + "};");

        return cluster;
    }

    protected TestCluster createCluster(int nodeCount) throws Throwable
    {
        TestCluster cluster = TestCluster.create(nodeCount);
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + nodeCount + "};");

        return cluster;
    }

    public static void assertRows(Object[][] actual, Object[]... expected)
    {
        Assert.assertEquals(rowsNotEqualErrorMessage(expected, actual),
                            expected.length, actual.length);

        for (int i = 0; i < expected.length; i++)
        {
            Object[] expectedRow = expected[i];
            Object[] actualRow = actual[i];
            Assert.assertTrue(rowsNotEqualErrorMessage(actual, expected),
                              Arrays.equals(expectedRow, actualRow));
        }
    }

    public static void assertRows(Iterator<Object[]> actual, Iterator<Object[]> expected)
    {
        while (actual.hasNext() && expected.hasNext())
        {
            Object[] expectedRow = expected.next();
            Object[] actualRow = actual.next();
            Assert.assertTrue(rowNotEqualErrorMessage(actualRow, expectedRow),
                              Arrays.equals(expectedRow, actualRow));
        }

        Assert.assertEquals(actual.hasNext(), expected.hasNext());
    }

    public static Object[][] toObjectArray(Iterator<Object[]> iter)
    {
        List<Object[]> res = new ArrayList<>();
        while (iter.hasNext())
            res.add(iter.next());

        return res.toArray(new Object[res.size()][]);
    }

    public static String rowNotEqualErrorMessage(Object[] actual, Object[] expected)
    {
        return String.format("Expected: %s\nActual:%s\n",
                             Arrays.toString(expected),
                             Arrays.toString(actual));
    }

    public static String rowsNotEqualErrorMessage(Object[][] actual, Object[][] expected)
    {
        return String.format("Expected: %s\nActual:%s\n",
                             rowsToString(expected),
                             rowsToString(actual));
    }

    public static String rowsToString(Object[][] rows)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        boolean isFirst = true;
        for (Object[] row : rows)
        {
            if (isFirst)
                isFirst = false;
            else
                builder.append(",");
            builder.append(Arrays.toString(row));
        }
        builder.append("]");
        return builder.toString();
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }
}
