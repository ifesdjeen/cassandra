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

import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointsForRange;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;

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
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

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

            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
            {
                return unsortedAddress;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
            {
                return false;
            }
        });

        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("ks", KeyspaceParams.nts(DC1, "3/1", DC2, "3/1"), SchemaLoader.standardCFMD("ks", "tbl"));
        ks = Keyspace.open("ks");
        cfs = ks.getColumnFamilyStore("tbl");
    }

    @Test
    public void checkPendingReplicasAreFiltered()
    {
        EndpointsForRange natural = EndpointsForRange.of(full(EP1), full(EP2), trans(EP3));
        EndpointsForRange pending = EndpointsForRange.of(full(EP4), full(EP5), trans(EP6));
        WritePathReplicaPlan replicaPlan = WritePathReplicaPlan.createReplicaPlan(ks, ConsistencyLevel.QUORUM, natural, pending, (a) -> true);

        Assert.assertEquals(EndpointsForRange.of(full(EP4), full(EP5)), replicaPlan.pendingReplicas());
    }

    private static WritePathReplicaPlan expected(EndpointsForRange all, EndpointsForRange initial)
    {
        return new WritePathReplicaPlan(ks, null, all, initial, EndpointsForRange.empty(all.range()));
    }

    private static WritePathReplicaPlan getSpeculationContext(EndpointsForRange replicas, int blockFor, Predicate<InetAddressAndPort> livePredicate)
    {
        return WritePathReplicaPlan.createReplicaPlan(ks, ConsistencyLevel.QUORUM, blockFor, replicas, EndpointsForRange.empty(replicas.range()), livePredicate);
    }

    private static void assertSpeculationReplicas(WritePathReplicaPlan expected, EndpointsForRange replicas, int blockFor, Predicate<InetAddressAndPort> livePredicate)
    {
        WritePathReplicaPlan actual = getSpeculationContext(replicas, blockFor, livePredicate);
        Assert.assertEquals(expected.allReplicas, actual.allReplicas);
        Assert.assertEquals(expected.targetReplicas, actual.targetReplicas);
    }

    private static Predicate<InetAddressAndPort> dead(InetAddressAndPort... endpoints)
    {
        Set<InetAddressAndPort> deadSet = Sets.newHashSet(endpoints);
        return ep -> !deadSet.contains(ep);
    }

    private static EndpointsForRange replicas(Replica... rr)
    {
        return EndpointsForRange.of(rr);
    }

    @Test
    public void checkSpeculationContext()
    {
        EndpointsForRange all = replicas(full(EP1), full(EP2), trans(EP3));
        // in happy path, transient replica should be classified as a backup
        assertSpeculationReplicas(expected(all,
                                           replicas(full(EP1), full(EP2))),
                                  replicas(full(EP1), full(EP2), trans(EP3)),
                                  2, dead());

        // if one of the full replicas is dead, they should all be in the initial contacts
        assertSpeculationReplicas(expected(all,
                                           replicas(full(EP1), trans(EP3))),
                                  replicas(full(EP1), full(EP2), trans(EP3)),
                                  2, dead(EP2));

        // block only for 1 full replica, use transient as backups
        assertSpeculationReplicas(expected(all,
                                           replicas(full(EP1))),
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
