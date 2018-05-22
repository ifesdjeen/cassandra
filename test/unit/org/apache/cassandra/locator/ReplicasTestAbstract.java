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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicasTestAbstract extends ReplicaCollectionTestBase
{
    @Test
    public void testFilter() throws Exception
    {
      ReplicaList list = ReplicaList.of(A, B, C);
      ReplicaCollection result = Replicas.filter(list, replica -> replica.getEndpoint().equals(FBUtilities.getLocalAddressAndPort()));
      testFilterAssertions(result);
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNullList()
    {
        Replicas.filter(null, Predicates.alwaysTrue());
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNullPredicate()
    {
        Replicas.filter(ReplicaList.empty(), null);
    }

    @Test
    public void testFilterOnEndpoints() throws Exception
    {
        ReplicaList list = ReplicaList.of(A, B, C);
        ReplicaCollection result = Replicas.filterOnEndpoints(list, endpoint -> endpoint.equals(FBUtilities.getLocalAddressAndPort()));
        testFilterAssertions(result);
    }

    @Test(expected = NullPointerException.class)
    public void testFilterOnEndpointsNullList()
    {
        Replicas.filterOnEndpoints(null, Predicates.alwaysTrue());
    }

    @Test(expected = NullPointerException.class)
    public void testFilterOnEndpointsNullPredicate()
    {
        Replicas.filterOnEndpoints(ReplicaList.empty(), null);
    }

    @Test
    public void testFilterOutLocalEndpoint() throws Exception
    {
        ReplicaList list = ReplicaList.of(BROADCAST_REPLICA, B, C);
        ReplicaCollection result = Replicas.filterOutLocalEndpoint(list);
        assertEquals(2, result.size());
        Iterator<Replica> i = result.iterator();
        assertEquals(B, i.next());
        assertEquals(C, i.next());
    }

    @Test(expected = NullPointerException.class)
    public void testFilterOutLocalEndpointNullList()
    {
        Replicas.filterOutLocalEndpoint(null);
    }

    private void testFilterAssertions(ReplicaCollection collection)
    {
        assertEquals(1, collection.size());
        assertEquals(A, collection.iterator().next());
        assertEquals(1, collection.getUnmodifiableCollection().size());
        assertEquals(A, collection.getUnmodifiableCollection().iterator().next());
        Iterator<Replica> i = collection.stream().iterator();
        assertEquals(A, i.next());
        assertFalse(i.hasNext());
        boolean threw = false;
        try
        {
            collection.getUnmodifiableCollection().add(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            collection.add(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            collection.add(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            collection.addAll(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            collection.removeEndpoint(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            collection.removeReplica(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        threw = false;
        try
        {
            collection.removeReplicas(null);
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);
    }

    @Test
    public void testConcatNaturalAndPending() throws Exception
    {
         boolean threw = false;
         try
         {
             Replicas.concatNaturalAndPending(ReplicaList.of(), ReplicaList.of(C));
         }
         catch (UnsupportedOperationException e)
         {
             threw = true;
         }
         assertTrue(threw);

        threw = false;
        try
        {
            Replicas.concatNaturalAndPending(ReplicaList.of(C), ReplicaList.of());
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        ReplicaCollection result = Replicas.concatNaturalAndPending(ReplicaList.of(A), ReplicaList.of(B));
        assertEquals(2, result.size());
        Iterator<Replica> i = result.iterator();
        assertEquals(A, i.next());
        assertEquals(B, i.next());

        threw = false;
        try
        {
            result.getUnmodifiableCollection();
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        i = result.stream().iterator();
        assertEquals(A, i.next());
        assertEquals(B, i.next());
    }

    @Test(expected = NullPointerException.class)
    public void testConcatNaturalAndPendingNullA() throws Exception
    {
        Replicas.concatNaturalAndPending(null, Replicas.empty());
    }

    @Test(expected = NullPointerException.class)
    public void testConcatNaturalAndPendingNullB() throws Exception
    {
        Replicas.concatNaturalAndPending( Replicas.empty(), null);
    }

    @Test
    public void testConcat() throws Exception
    {
        ReplicaCollection result = Replicas.concat(ImmutableList.of(ReplicaList.of(A), ReplicaList.of(B)));
        assertEquals(2, result.size());
        Iterator<Replica> i = result.iterator();
        assertEquals(A, i.next());
        assertEquals(B, i.next());

        boolean threw = false;
        try
        {
            result.getUnmodifiableCollection();
        }
        catch (UnsupportedOperationException e)
        {
            threw = true;
        }
        assertTrue(threw);

        i = result.stream().iterator();
        assertEquals(A, i.next());
        assertEquals(B, i.next());
    }

    @Test(expected = NullPointerException.class)
    public void testConcatNull() throws Exception
    {
        Replicas.concat(null);
    }

    @Test(expected = NullPointerException.class)
    public void testOfNull() throws Exception
    {
        Replicas.of((Replica)null);
    }

    @Test
    public void testOfReplica() throws Exception
    {
        ReplicaCollection result = Replicas.of(A);
        assertEquals(1, result.size());
        assertEquals(A, result.iterator().next());
        assertEquals(1, result.getUnmodifiableCollection().size());
        assertEquals(A, result.stream().iterator().next());
    }

    @Test
    public void testOfReplicaCollection() throws Exception
    {
        ReplicaCollection result = Replicas.of(Collections.singleton(A));
        assertEquals(1, result.size());
        assertEquals(A, result.iterator().next());
        assertEquals(1, result.getUnmodifiableCollection().size());
        assertEquals(A, result.stream().iterator().next());
    }

    @Test(expected = NullPointerException.class)
    public void testOfReplicaCollectionNull() throws Exception
    {
        Replicas.of((Collection<Replica>)null);
    }

    @Test
    public void testEmpty() throws Exception
    {
        assertTrue(Replicas.empty().isEmpty());
        assertEquals(0, Replicas.empty().size());
        assertEquals(0, Replicas.empty().getUnmodifiableCollection().size());
        assertFalse(Replicas.empty().iterator().hasNext());
        assertFalse(Replicas.empty().stream().iterator().hasNext());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCheckFullTransientReplica()
    {
        Replicas.checkFull(C);
    }

    @Test
    public void testCheckFullFullReplica()
    {
        Replicas.checkFull(A);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCheckFullIterableTransientReplica()
    {
        Replicas.checkFull(Collections.singleton(C));
    }

    @Test
    public void testCheckFullIterableFullReplica()
    {
        Replicas.checkFull(Collections.singleton(A));
    }

    @Test
    public void testStringify()
    {
        List<String> result = Replicas.stringify(Replicas.of(A), true);
        assertEquals(result.size(), 1);
        assertEquals(A.getEndpoint().getHostAddress(true), result.get(0));
        result = Replicas.stringify(Replicas.of(A), false);
        assertEquals(result.size(), 1);
        assertEquals(A.getEndpoint().getHostAddress(false), result.get(0));
    }
}
