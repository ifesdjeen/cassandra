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

package org.apache.cassandra.repair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.AsymmetricSyncRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.UUIDGen;

public class RemoteSyncTaskTest extends AbstractRepairTest
{
    private static final RepairJobDesc DESC = new RepairJobDesc(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID(), "ks", "tbl", ALL_RANGES);
    private static final List<Range<Token>> RANGE_LIST = ImmutableList.of(RANGE1);

    private static class InstrumentedRemoteSyncTask extends RemoteSyncTask
    {
        public InstrumentedRemoteSyncTask(InetAddressAndPort e1, InetAddressAndPort e2, Predicate<InetAddressAndPort> isTransient)
        {
            super(DESC, new TreeResponse(e1, null), new TreeResponse(e2, null), isTransient, PreviewKind.NONE);
        }

        RepairMessage sentMessage = null;
        InetAddressAndPort sentTo = null;

        @Override
        void sendRequest(RepairMessage request, InetAddressAndPort to)
        {
            Assert.assertNull(sentMessage);
            Assert.assertNotNull(request);
            Assert.assertNotNull(to);
            sentMessage = request;
            sentTo = to;
        }
    }

    @Test
    public void normalSync()
    {
        InstrumentedRemoteSyncTask syncTask = new InstrumentedRemoteSyncTask(PARTICIPANT1,  PARTICIPANT2, e -> false);
        syncTask.startSync(RANGE_LIST);

        Assert.assertNotNull(syncTask.sentMessage);
        Assert.assertSame(SyncRequest.class, syncTask.sentMessage.getClass());
        Assert.assertEquals(PARTICIPANT1, syncTask.sentTo);
    }

    @Test
    public void transientSync()
    {
        InstrumentedRemoteSyncTask syncTask;

        // r1 is transient
        syncTask = new InstrumentedRemoteSyncTask(PARTICIPANT1, PARTICIPANT2, PARTICIPANT1::equals);
        syncTask.startSync(RANGE_LIST);

        Assert.assertNotNull(syncTask.sentMessage);
        Assert.assertSame(AsymmetricSyncRequest.class, syncTask.sentMessage.getClass());
        Assert.assertEquals(PARTICIPANT2, syncTask.sentTo);


        // r2 is transient
        syncTask = new InstrumentedRemoteSyncTask(PARTICIPANT1, PARTICIPANT2, PARTICIPANT2::equals);
        syncTask.startSync(RANGE_LIST);

        Assert.assertNotNull(syncTask.sentMessage);
        Assert.assertSame(AsymmetricSyncRequest.class, syncTask.sentMessage.getClass());
        Assert.assertEquals(PARTICIPANT1, syncTask.sentTo);
    }

    /**
     * If both endpoints are transient, we should throw an exception
     */
    @Test (expected = IllegalStateException.class)
    public void invalidTransientSync()
    {
        InstrumentedRemoteSyncTask syncTask = new InstrumentedRemoteSyncTask(PARTICIPANT1,  PARTICIPANT2, e -> true);
        syncTask.startSync(RANGE_LIST);
    }
}
