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

package org.apache.cassandra.locator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ReplicaListTest extends ReplicaCollectionTestBase
{
    @Test
    public void testCollector()
    {
        ReplicaList list = ReplicaList.of(A, B, C);
        assertEquals(list, list.stream().filter(Predicates.alwaysTrue()).collect(ReplicaList.COLLECTOR));

        ReplicaList abList = new ReplicaList();
        abList.add(A);
        abList.add(B);
        ReplicaList cList = new ReplicaList();
        cList.add(C);
        ReplicaList combined = ReplicaList.COLLECTOR.combiner().apply(abList, cList);
        assertEquals(list, combined);
        assertTrue(combined == abList);
    }

    @Test
    public void testEquals()
    {
        ReplicaList aList = ReplicaList.of(A, B, C);
        assertTrue(aList.equals(aList));
        ReplicaList aList2 = ReplicaList.of(A, B, C);
        assertTrue(aList.equals(aList2));
        ReplicaList bList = ReplicaList.of(B, C);
        assertFalse(aList.equals(bList));

        assertFalse(aList.equals(null));
        assertFalse(aList.equals(new Object()));
        //This sucks, but it's because List is not equal with this either
        assertFalse(aList.equals(ImmutableList.of(A, B, C)));
    }

    @Test
    public void testHashCode()
    {
        ReplicaList aList = ReplicaList.of(A, B, C);
        assertEquals(aList.hashCode(), aList.hashCode());
        ReplicaList aList2 = ReplicaList.of(A, B, C);
        assertEquals(aList.hashCode(), aList2.hashCode());
        ReplicaList bList = ReplicaList.of(B, C);
        assertNotEquals(aList.hashCode(), bList.hashCode());
        //Just co-incidence we share hash code behavior with java.util.AbstractList
        assertEquals(aList.hashCode(), ImmutableList.of(A, B, C).hashCode());
    }

    @Test
    public void testToString()
    {
        assertEquals(ReplicaList.of(A, B, C).toString(), ImmutableList.of(A, B, C).toString());
    }

    @Test(expected = NullPointerException.class)
    public void testAddNull()
    {
        new ReplicaList().add(null);
    }

    @Test
    public void testAdd()
    {
        ReplicaList list = new ReplicaList(3);
        list.add(A);
        assertEquals(1, list.size());
        assertEquals( A, list.get(0));

        boolean threw = false;
        try
        {
            list.get(1);
        }
        catch (IndexOutOfBoundsException e)
        {
            threw = true;
        }
        assertTrue(threw);

        list.add(B);
        list.add(C);
        assertEquals( 3, list.size());
        assertEquals(A, list.get(0));
        assertEquals(B, list.get(1));
        assertEquals(C, list.get(2));
    }

    @Test(expected = NullPointerException.class)
    public void testAddAllNull()
    {
        new ReplicaList().addAll(null);
    }

    @Test
    public void testAddAll()
    {
        ReplicaList list = new ReplicaList(new LinkedList<>());
        list.addAll(ImmutableList.of(A, B, C));
        assertEquals(3, list.size());
        assertEquals(A, list.get(0));
        assertEquals(B, list.get(1));
        assertEquals(C, list.get(2));
    }

    @Test
    public void testIterator()
    {
        assertFalse(new ReplicaList().iterator().hasNext());
        ReplicaList list = new ReplicaList(ImmutableList.of(A));
        Iterator<Replica> i = list.iterator();
        assertTrue(i.hasNext());
        assertEquals(A, i.next());
        assertFalse(i.hasNext());
        list.add(B);
        list.add(C);
        i = list.iterator();
        assertTrue(i.hasNext());
        assertEquals(A, i.next());
        assertTrue(i.hasNext());
        assertEquals(B, i.next());
        assertTrue(i.hasNext());
        assertEquals(C, i.next());
        assertFalse(i.hasNext());
    }

    @Test
    public void testRemoveEndpoint()
    {
        ReplicaList set = new ReplicaList();
        set.addAll(ImmutableList.of(A, B, Replica.trans(B.getEndpoint(), B.getRange()), C));
        set.removeEndpoint(B.getEndpoint());
        List<Replica> check = Lists.newArrayList(A, C);
        for (Replica r : set)
        {
            assertTrue(check.remove(r));
        }
        assertTrue(check.isEmpty());
    }

    @Test
    public void removeEndpoint()
    {
        ReplicaList rlist = new ReplicaList();
        rlist.add(ReplicaUtils.full(EP1, range(0, 100)));
        rlist.add(ReplicaUtils.full(EP2, range(0, 100)));
        rlist.add(ReplicaUtils.full(EP3, range(0, 100)));

        assertTrue(rlist.containsEndpoint(EP1));
        assertEquals(3, rlist.size());
        rlist.removeEndpoint(EP1);
        assertFalse(rlist.containsEndpoint(EP1));
        assertEquals(2, rlist.size());
    }

    @Test
    public void removeReplica()
    {
        ReplicaList list = new ReplicaList(Replicas.of(ImmutableList.of(A, B, C)));
        assertEquals( 3, list.size());
        list.removeReplica(B);
        assertEquals(2, list.size());
        assertEquals(A, list.get(0));
        assertEquals(C, list.get(1));
    }

    @Test(expected = NullPointerException.class)
    public void containsEndpointNull()
    {
        new ReplicaList().containsEndpoint(null);
    }

    @Test
    public void testContainsEndpoint() throws Exception
    {
        ReplicaList list = new ReplicaList();
        list.addAll(ImmutableList.of(A, B, C));
        assertTrue(list.containsEndpoint(B.getEndpoint()));
        assertTrue(list.containsEndpoint(A.getEndpoint()));
        assertTrue(list.containsEndpoint(C.getEndpoint()));
        assertFalse(list.containsEndpoint(InetAddressAndPort.getByName("0.0.0.0")));
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNullPredicate()
    {
        new ReplicaList().filter((com.google.common.base.Predicate[])null);
    }

    @Test
    public void testFilter()
    {
        ReplicaList list = new ReplicaList(new ReplicaList(ImmutableList.of(A, B, C)));
        assertEquals(3, list.size());
        ReplicaList filtered = list.filter(replica -> replica != B);
        assertEquals(2, filtered.size());
        assertEquals(A, filtered.get(0));
        assertEquals(C, filtered.get(1));
    }

    @Test
    public void testSort()
    {
        ReplicaList list = new ReplicaList(ImmutableList.of(B, A, C));
        list.sort(Comparator.naturalOrder());
        ArrayList list2 = Lists.newArrayList(B, A, C);
        list2.sort(Comparator.naturalOrder());
        assertEquals(new ReplicaList(list2), list);
    }

    @Test(expected = NullPointerException.class)
    public void testIntersectEndpointsNullA()
    {
        ReplicaList.intersectEndpoints(null, new ReplicaList());
    }

    @Test(expected = NullPointerException.class)
    public void testIntersectEndpointsNullB()
    {
        ReplicaList.intersectEndpoints(new ReplicaList(), null);
    }

    @Test
    public void testIntersectEndpoints()
    {
        ReplicaList aList = ReplicaList.of(A, A, B, B);
        ReplicaList bList = ReplicaList.of(B);
        ReplicaList result = ReplicaList.intersectEndpoints(aList, bList);
        assertEquals(2, result.size());
        assertEquals(B, result.get(0));
        assertEquals(B, result.get(1));
    }

    @Test
    public void testOfEmpty()
    {
        assertTrue(ReplicaList.of().size() == 0);
        assertFalse(ReplicaList.of().iterator().hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void testOfReplicaNull()
    {
        ReplicaList.of((Replica)null);
    }

    @Test
    public void testOfReplica()
    {
       ReplicaList list = ReplicaList.of(A);
       assertEquals(1, list.size());
       assertEquals( A, list.get(0));
    }

    @Test
    public void testOfReplicas()
    {
        ReplicaList list = ReplicaList.of(A, B, C);
        assertEquals( 3, list.size());
        assertEquals( A, list.get(0));
        assertEquals( B, list.get(1));
        assertEquals( C, list.get(2));
    }

    @Test
    public void testSubList()
    {
        ReplicaList list = ReplicaList.of(A, B, C);
        ReplicaList sublist = list.subList(1, 2);
        assertEquals( 1, sublist.size());
        assertEquals(B, sublist.get(0));
    }

    @Test
    public void testToDummyList()
    {
       ReplicaList dummies = ReplicaList.toDummyList(ImmutableList.of(A.getRange(), B.getRange(), C.getRange()));
       List<Range<Token>> ranges = Lists.newArrayList(A.getRange(), B.getRange(), C.getRange());
       for (Replica r : dummies)
       {
           assertEquals("0.0.0.0:0", r.getEndpoint().getHostAddress(true));
           assertEquals(ranges.remove(0), r.getRange());
       }
    }
}
