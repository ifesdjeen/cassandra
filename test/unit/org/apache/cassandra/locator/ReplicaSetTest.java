///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.locator;
//
//import java.util.Iterator;
//import java.util.Set;
//
//import com.google.common.base.Predicates;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Sets;
//import org.junit.Assert;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotEquals;
//import static org.junit.Assert.assertTrue;
//
//public class ReplicaSetTest extends ReplicaCollectionTestBase
//{
//
//    @Test
//    public void testCollector()
//    {
//        ReplicaSet set = ReplicaSet.of(A, B, C);
//        assertEquals(set, set.stream().filter(Predicates.alwaysTrue()).collect(ReplicaSet.COLLECTOR));
//
//        ReplicaSet abSet = new ReplicaSet();
//        abSet.add(A);
//        abSet.add(B);
//        ReplicaSet cSet = new ReplicaSet();
//        cSet.add(C);
//        ReplicaSet combined = ReplicaSet.COLLECTOR.combiner().apply(abSet, cSet);
//        assertEquals(set, combined);
//        assertTrue(abSet == combined);
//
//        ReplicaSet aSet = new ReplicaSet();
//        aSet.add(A);
//        ReplicaSet bcSet = new ReplicaSet();
//        bcSet.add(B);
//        bcSet.add(C);
//        combined = ReplicaSet.COLLECTOR.combiner().apply(aSet, bcSet);
//        assertEquals(set, combined);
//        assertTrue(bcSet == combined);
//    }
//
//    @Test
//    public void testEquals()
//    {
//        ReplicaSet aSet = ReplicaSet.of(A, B, C);
//        assertTrue(aSet.equals(aSet));
//        ReplicaSet aSet2 = ReplicaSet.of(A, B, C);
//        assertTrue(aSet.equals(aSet2));
//        ReplicaSet bSet = ReplicaSet.of(B, C);
//        assertFalse(aSet.equals(bSet));
//        //Not sensitive to order
//        assertTrue(aSet.equals(ReplicaSet.of(C, B, A)));
//
//        assertFalse(aSet.equals(null));
//        assertFalse(aSet.equals(new Object()));
//        //This sucks, but it's because Set is not equal with this either
//        assertFalse(aSet.equals(ImmutableSet.of(A, B, C)));
//    }
//
//    @Test
//    public void testHashCode()
//    {
//        ReplicaSet aSet = ReplicaSet.of(A, B, C);
//        assertEquals(aSet.hashCode(), aSet.hashCode());
//        ReplicaSet aSet2 = ReplicaSet.of(A, B, C);
//        assertEquals(aSet.hashCode(), aSet2.hashCode());
//        ReplicaSet bSet = ReplicaSet.of(B, C);
//        assertNotEquals(aSet.hashCode(), bSet.hashCode());
//        assertEquals(aSet.hashCode(), ReplicaSet.of(C, B, A).hashCode());
//        //Just co-incidence we share hash code behavior with java.util.AbstractSet
//        assertEquals(aSet.hashCode(), ImmutableSet.of(A, B, C).hashCode());
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testAddNull()
//    {
//        new ReplicaSet().add(null);
//    }
//
//    @Test
//    public void testAdd()
//    {
//        ReplicaSet set = new ReplicaSet();
//        set.add(A);
//        set.add(B);
//        set.add(C);
//        Set<Replica> check = Sets.newHashSet(A, B, C);
//        for (Replica r : set)
//        {
//            assertTrue(check.remove(r));
//        }
//        assertTrue(check.isEmpty());
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testAddAllNull()
//    {
//        new ReplicaSet().addAll(null);
//    }
//
//    @Test
//    public void testAddAll()
//    {
//        ReplicaSet set = new ReplicaSet();
//        set.addAll(ImmutableSet.of(A, B, C));
//        Set<Replica> check = Sets.newHashSet(A, B, C);
//        for (Replica r : set)
//        {
//            assertTrue(check.remove(r));
//        }
//        assertTrue(check.isEmpty());
//    }
//
//    @Test
//    public void testRemoveEndpoint()
//    {
//        ReplicaSet set = new ReplicaSet();
//        set.addAll(ImmutableSet.of(A, B, Replica.trans(B.endpoint(), B.range()), C));
//        set.removeEndpoint(B.endpoint());
//        Set<Replica> check = Sets.newHashSet(A, C);
//        for (Replica r : set)
//        {
//            assertTrue(check.remove(r));
//        }
//        assertTrue(check.isEmpty());
//    }
//
//    @Test
//    public void removeEndpoint()
//    {
//        ReplicaSet rset = new ReplicaSet();
//        rset.add(ReplicaUtils.full(EP1, range(0, 100)));
//        rset.add(ReplicaUtils.full(EP2, range(0, 100)));
//        rset.add(ReplicaUtils.full(EP3, range(0, 100)));
//
//        Assert.assertTrue(rset.containsEndpoint(EP1));
//        Assert.assertEquals(3, rset.size());
//        rset.removeEndpoint(EP1);
//        assertFalse(rset.containsEndpoint(EP1));
//        Assert.assertEquals(2, rset.size());
//    }
//
//    @Test
//    public void removeReplica()
//    {
//        ReplicaSet set = new ReplicaSet(Replicas.of(ImmutableSet.of(A, B, C)));
//        assertEquals( 3, set.size());
//        set.removeReplica(B);
//        assertEquals(2, set.size());
//        Set<Replica> check = Sets.newHashSet(A, C);
//        Iterator<Replica> i = set.iterator();
//        assertTrue(check.remove(i.next()));
//        assertTrue(check.remove(i.next()));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void differenceOnEndpointNull()
//    {
//        ReplicaSet set = new ReplicaSet();
//        set.differenceOnEndpoint(null);
//    }
//
//    @Test
//    public void differenceOnEndpoint()
//    {
//        ReplicaSet set = ReplicaSet.of(A, B, Replica.full(B.endpoint(), C.range()), Replica.full(C.endpoint(), C.range()), Replica.full(C.endpoint(), B.range()));
//        assertEquals(0, set.differenceOnEndpoint(set).size());
//        ReplicaSet set2 = ReplicaSet.of(B);
//        ReplicaSet difference = set.differenceOnEndpoint(set2);
//        assertEquals( 3, difference.size());
//        Set<Replica> check = Sets.newHashSet(A, Replica.full(C.endpoint(), C.range()), Replica.full(C.endpoint(), B.range()));
//        Iterator<Replica> i = difference.iterator();
//        assertTrue(check.remove(i.next()));
//        assertTrue(check.remove(i.next()));
//        assertTrue(check.remove(i.next()));
//    }
//}
