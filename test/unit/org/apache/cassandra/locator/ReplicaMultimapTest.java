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
//
//import java.util.List;
//import java.util.Map;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertTrue;
//
//public class ReplicaMultimapTest extends ReplicaCollectionTestBase
//{
//    @Test
//    public void testEquals()
//    {
//        ReplicaMultimap<Replica, ReplicaList> map = ReplicaMultimap.list();
//        assertTrue(map.equals(map));
//        assertFalse(map.equals(null));
//        ReplicaMultimap<Replica, ReplicaList> map2 = ReplicaMultimap.list();
//        assertTrue(map.equals(map2));
//        map.put(A, A);
//        assertFalse(map.equals(map2));
//        map2.put(A, A);
//        assertTrue(map.equals(map2));
//        map.put(A, A);
//        assertFalse(map.equals(map2));
//        map2.put(A, A);
//        assertTrue(map.equals(map2));
//        ReplicaMultimap<Replica, ReplicaSet> map3 = ReplicaMultimap.set();
//        map.asMap().clear();
//        assertFalse(map.equals(map3));
//        map.put(A, A);
//        map3.put(A, A);
//        assertFalse(map.equals(map3));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testGetNull()
//    {
//        ReplicaMultimap.list().get(null);
//    }
//
//    //Ergonomics of this are a little weird compared to Multimap but not going to change it for now
//    @Test(expected = UnsupportedOperationException.class)
//    public void testGetListIsImmutable()
//    {
//        ReplicaMultimap.list().get(A).add(A);
//    }
//
//    @Test(expected = UnsupportedOperationException.class)
//    public void testGetSetIsImmutable()
//    {
//        ReplicaMultimap.set().get(A).add(A);
//    }
//
//    @Test
//    public void testGetList()
//    {
//        ReplicaMultimap<Replica, ReplicaList> map = ReplicaMultimap.list();
//        map.put(A, A);
//        assertTrue(map.get(A) instanceof ReplicaList);
//        assertEquals(ReplicaList.of(A), map.get(A));
//    }
//
//    @Test
//    public void testGetSet()
//    {
//        ReplicaMultimap<Replica, ReplicaSet> map = ReplicaMultimap.set();
//        map.put(A, A);
//        assertTrue(map.get(A) instanceof ReplicaSet);
//        assertEquals(ReplicaSet.of(A), map.get(A));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void putNullKey()
//    {
//        ReplicaMultimap.list().put(null, A);
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void putNullValue()
//    {
//        ReplicaMultimap.list().put(A, null);
//    }
//
//    @Test
//    public void testPut()
//    {
//        ReplicaMultimap<Replica, ReplicaList> map = ReplicaMultimap.list();
//        map.put(A, A);
//        assertEquals(1, map.asMap().size());
//        assertEquals(ReplicaList.of(A), map.get(A));
//        map.put(A, B);
//        assertEquals(1, map.asMap().size());
//        assertEquals(ReplicaList.of(A, B), map.get(A));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testPutAllCollectionNullKey()
//    {
//        ReplicaMultimap.list().putAll(null, ReplicaList.of());
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testPutAllCollectionNullValue()
//    {
//        ReplicaMultimap.list().putAll(A, null);
//    }
//
//    @Test
//    public void testPutAllCollection()
//    {
//        ReplicaList sourceList = ReplicaList.of(A, B, C);
//        ReplicaMultimap<Replica, ReplicaList> map = ReplicaMultimap.list();
//        map.putAll(A, sourceList);
//        assertEquals(sourceList, map.get(A));
//        assertFalse(map.get(A) == sourceList);
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testPutAllNullMap()
//    {
//        ReplicaMultimap.list().putAll(null);
//    }
//
//    @Test
//    public void testPutAll()
//    {
//        ReplicaList sourceList = ReplicaList.of(A, B, C);
//        ReplicaMultimap<Replica, ReplicaSet> map = ReplicaMultimap.set();
//        map.putAll(A, sourceList);
//        ReplicaMultimap<Replica, ReplicaList> map2 = ReplicaMultimap.list();
//        map2.putAll(map);
//        assertTrue(map2.get(A).allMatch(Sets.newHashSet(C, A, B)::contains));
//    }
//
//    @Test
//    public void testEntries()
//    {
//        ReplicaMultimap<Replica, ReplicaList> map = ReplicaMultimap.list();
//        map.putAll(A, ReplicaList.of(A, B, C));
//        List<Replica> check = Lists.newArrayList(A, B, C);
//        for (Map.Entry<Replica, Replica> entry : map.entries())
//        {
//            assertEquals(A, entry.getKey());
//            assertTrue(check.remove(entry.getValue()));
//        }
//        assertTrue(check.isEmpty());
//    }
//}
