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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicaCollectionTest extends ReplicaCollectionTestBase
{
    @Test
    public void testAsEndpoints()
    {
        ReplicaList replicaList = ReplicaList.of(A, B, C);
        Iterator<InetAddressAndPort> i = replicaList.asEndpoints().iterator();
        assertEquals(A.getEndpoint(), i.next());
        assertEquals(B.getEndpoint(), i.next());
        assertEquals(C.getEndpoint(), i.next());
    }

    @Test
    public void testAsEndpointList()
    {
        ReplicaList replicaList = ReplicaList.of(A, B, C);
        List<InetAddressAndPort> list = replicaList.asEndpointList();
        Iterator<InetAddressAndPort> i = list.iterator();
        assertEquals(A.getEndpoint(), i.next());
        assertEquals(B.getEndpoint(), i.next());
        assertEquals(C.getEndpoint(), i.next());
        list.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAsUnmodifiableEndpointCollectionUnmodifiable()
    {
        ReplicaList.of(A, B, C).asUnmodifiableEndpointCollection().clear();
    }

    @Test
    public void testAsUnmodifiableEndpointCollection()
    {
        Iterator<InetAddressAndPort> i = ReplicaList.of(A, B, C).asUnmodifiableEndpointCollection().iterator();
        assertEquals(A.getEndpoint(), i.next());
        assertEquals(B.getEndpoint(), i.next());
        assertEquals(C.getEndpoint(), i.next());
        assertFalse(i.hasNext());
    }

    @Test
    public void testAsRanges()
    {
        Iterator<Range<Token>> i = ReplicaList.of(A, B, C).asRanges().iterator();
        assertEquals(A.getRange(), i.next());
        assertEquals(B.getRange(), i.next());
        assertEquals(C.getRange(), i.next());
        assertFalse(i.hasNext());
    }

    @Test
    public void testAsRangeSet()
    {
        Set<Range<Token>> ranges = ReplicaList.of(A, B, C).asRangeSet();
        assertEquals(Sets.newHashSet(A, B, C).stream().map(Replica::getRange).collect(Collectors.toSet()), ranges);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAsUnmodifiableRangeCollectionUnmodifiable()
    {
        ReplicaList.of(A, B, C).asUnmodifiableRangeCollection().clear();
    }

    @Test
    public void testAsUnmodifiableRangeCollection()
    {
        Collection<Range<Token>> ranges = ReplicaList.of(A, B, C).asUnmodifiableRangeCollection();
        assertTrue(Iterators.elementsEqual(Lists.newArrayList(A, B, C).stream().map(Replica::getRange).collect(Collectors.toList()).iterator(), ranges.iterator()));
    }

    @Test
    public void testFullRanges()
    {
        assertTrue(Iterators.elementsEqual(Lists.newArrayList(A, B).stream().map(Replica::getRange).collect(Collectors.toList()).iterator(), ReplicaList.of(A, B, C).fullRanges().iterator()));
    }

    @Test
    public void testTransientRanges()
    {
        assertTrue(Iterators.elementsEqual(Lists.newArrayList(C.getRange()).iterator(), ReplicaList.of(A, B, C).transientRanges().iterator()));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsEndpointNull()
    {
        ReplicaSet.of().containsEndpoint(null);
    }

    @Test
    public void testContainsEndpoint()
    {
        ReplicaSet set = ReplicaSet.of(A, B, C);
        assertTrue(Stream.of(A, B, C).map(Replica::getEndpoint).allMatch(set::containsEndpoint));
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveReplicasNull()
    {
        ReplicaSet.of().removeEndpoint(null);
    }

    @Test
    public void testRemoveReplicas()
    {
        ReplicaSet set = ReplicaSet.of(A, B, Replica.full(C.getEndpoint(), C.getRange()));
        set.removeReplicas(Replicas.of(B));
        assertEquals(ReplicaSet.of(A, Replica.full(C.getEndpoint(), C.getRange())), set);
    }

    @Test
    public void testIsEmpty()
    {
        ReplicaSet set = ReplicaSet.of(A);
        assertFalse(set.isEmpty());
        set.removeReplica(A);
        assertTrue(set.isEmpty());
    }

    @Test
    public void testToString()
    {
        ReplicaList list = ReplicaList.of(B, C);
        assertEquals("[Full(127.0.0.2:7000,(1,2]), Transient(127.0.0.3:7000,(2,3])]", list.toString());
    }

    @Test(expected = NullPointerException.class)
    public void testNoneMatchNull()
    {
        ReplicaList.of().noneMatch(null);
    }

    @Test
    public void testNoneMatch()
    {
        ReplicaList list = ReplicaList.of(A, B, C);
        assertTrue(list.noneMatch(Predicates.alwaysFalse()));
        assertFalse(list.noneMatch(C::equals));
    }

    @Test(expected = NullPointerException.class)
    public void testAnyMatchNull()
    {
        ReplicaList.of().anyMatch(null);
    }

    @Test
    public void testAnyMatch()
    {
        ReplicaList list = ReplicaList.of(A, B, C);
        assertTrue(list.anyMatch(C::equals));
        assertFalse(list.anyMatch(Predicates.alwaysFalse()));
    }

    @Test(expected = NullPointerException.class)
    public void testAllMatchNull()
    {
        ReplicaList.of().allMatch(null);
    }

    @Test
    public void testAllMatch()
    {
        ReplicaList list = ReplicaList.of(A, A, A);
        assertTrue(list.allMatch(A::equals));
        assertFalse(list.allMatch(C::equals));
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNullPredicates()
    {
        ReplicaList.of().filter(null, ReplicaList::new);
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNullCollector()
    {
        ReplicaList.of().filter(new java.util.function.Predicate[] { Predicates.alwaysTrue() }, null);
    }

    @Test
    public void testFilter()
    {
        ReplicaList result = ReplicaList.of(A, B, C).filter(new java.util.function.Predicate[] { Predicates.alwaysTrue() }, ReplicaList::new);
        assertEquals(ReplicaList.of(A, B, C), result);
        assertTrue(ReplicaList.of(A, B, C).filter(new java.util.function.Predicate[] { Predicates.alwaysFalse() }, ReplicaList::new).isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testCountNull()
    {
        ReplicaList.of().count(null);
    }

    @Test
    public void testCount()
    {
        assertEquals(1, ReplicaList.of(A, B, C).count(B::equals));
        assertEquals(2, ReplicaList.of(A, B, C).count(replica -> replica == A || replica == C));
    }

    @Test(expected = NullPointerException.class)
    public void testFindFirstNull()
    {
        ReplicaList.of().findFirst(null);
    }

    @Test
    public void testFindFirst()
    {
        assertEquals(B, ReplicaList.of(A, B, C).findFirst(B::equals).get());
        assertFalse(ReplicaList.of(A, B, C).findFirst(Predicates.alwaysFalse()).isPresent());
    }
}
