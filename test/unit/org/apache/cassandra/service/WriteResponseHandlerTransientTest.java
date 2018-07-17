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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.AbstractWriteResponseHandler.SpeculationContext;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.locator.ReplicaUtils.trans;

public class WriteResponseHandlerTransientTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;

    static final InetAddressAndPort EP1;
    static final InetAddressAndPort EP2;
    static final InetAddressAndPort EP3;
    static final InetAddressAndPort EP4;
    static final InetAddressAndPort EP5;
    static final InetAddressAndPort EP6;

    static final String DC1 = "datacenter1";
    static final String DC2 = "datacenter2";

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.1.0.1");
            EP2 = InetAddressAndPort.getByName("127.1.0.2");
            EP3 = InetAddressAndPort.getByName("127.1.0.3");
            EP4 = InetAddressAndPort.getByName("127.2.0.4");
            EP5 = InetAddressAndPort.getByName("127.2.0.5");
            EP6 = InetAddressAndPort.getByName("127.2.0.6");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        SchemaLoader.loadSchema();

        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.1"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.1"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return null;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.address.getAddress();
                if (address[1] == 1)
                    return DC1;
                else
                    return DC2;
            }

            public ReplicaList getSortedListByProximity(InetAddressAndPort address, ReplicaCollection unsortedAddress)
            {
                return null;
            }

            public void sortByProximity(InetAddressAndPort address, ReplicaList replicas)
            {

            }

            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(ReplicaList merged, ReplicaList l1, ReplicaList l2)
            {
                return false;
            }
        });

        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("ks", KeyspaceParams.nts(DC1, 3, DC2, 3), SchemaLoader.standardCFMD("ks", "tbl"));
        ks = Keyspace.open("ks");
        cfs = ks.getColumnFamilyStore("tbl");
    }

    private static AbstractWriteResponseHandler writeResponseHandler(ReplicaList natural, ReplicaList pending, ConsistencyLevel cl)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(natural, pending, cl, ()->{}, WriteType.SIMPLE, 0, null);
    }

    @Test
    public void checkPendingReplicasAreFiltered()
    {
        ReplicaList natural = ReplicaList.of(full(EP1), full(EP2), trans(EP3));
        ReplicaList pending = ReplicaList.of(full(EP4), full(EP5), trans(EP6));
        AbstractWriteResponseHandler handler = writeResponseHandler(natural, pending, ConsistencyLevel.QUORUM);

        Assert.assertEquals(2, handler.pendingReplicas.size());
        Assert.assertTrue(handler.pendingReplicas.containsEndpoint(EP4));
        Assert.assertTrue(handler.pendingReplicas.containsEndpoint(EP5));
        Assert.assertFalse(handler.pendingReplicas.containsEndpoint(EP6));
    }

    private static SpeculationContext expected(ReplicaList initial, ReplicaList backups)
    {
        return new SpeculationContext(initial, backups, null, null, DC1);
    }

    private static SpeculationContext getSpeculationContext(ReplicaList replicas, int blockFor, Predicate<InetAddressAndPort> livePredicate)
    {
        return AbstractWriteResponseHandler.createSpeculationContext(replicas, ConsistencyLevel.QUORUM, null, null, DC1, blockFor, livePredicate);
    }

    private static void assertSpeculationReplicas(SpeculationContext expected, ReplicaList replicas, int blockFor, Predicate<InetAddressAndPort> livePredicate)
    {
        SpeculationContext actual = getSpeculationContext(replicas, blockFor, livePredicate);
        Assert.assertEquals(expected.initial, actual.initial);
        Assert.assertEquals(expected.backups, actual.backups);
    }

    private static Predicate<InetAddressAndPort> dead(InetAddressAndPort... endpoints)
    {
        Set<InetAddressAndPort> deadSet = Sets.newHashSet(endpoints);
        return ep -> !deadSet.contains(ep);
    }

    private static ReplicaList replicas(Replica... rr)
    {
        return new ReplicaList(Lists.newArrayList(rr));
    }

    @Test
    public void checkSpeculationContext()
    {
        // in happy path, transient replica should be classified as a backup
        assertSpeculationReplicas(expected(replicas(full(EP1), full(EP2)),
                                           replicas(trans(EP3))),
                                  replicas(full(EP1), full(EP2), trans(EP3)),
                                  2, dead());

        // if one of the full replicas is dead, they should all be in the initial contacts
        assertSpeculationReplicas(expected(replicas(full(EP1), trans(EP3)),
                                           replicas()),
                                  replicas(full(EP1), full(EP2), trans(EP3)),
                                  2, dead(EP2));

        // block only for 1 full replica, use transient as backups
        assertSpeculationReplicas(expected(replicas(full(EP1)),
                                           replicas(trans(EP3))),
                                  replicas(full(EP1), full(EP2), trans(EP3)),
                                  1, dead(EP2));

    }

    @Test (expected = UnavailableException.class)
    public void noFullReplicas()
    {
        getSpeculationContext(replicas(full(EP1), trans(EP2), trans(EP3)), 2, dead(EP1));
    }

    @Test (expected = UnavailableException.class)
    public void notEnoughTransientReplicas()
    {
        getSpeculationContext(replicas(full(EP1), trans(EP2), trans(EP3)), 2, dead(EP2, EP3));
    }
}
