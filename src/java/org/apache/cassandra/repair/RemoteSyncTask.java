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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.AsymmetricSyncRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

/**
 * RemoteSyncTask sends {@link SyncRequest} to remote(non-coordinator) node
 * to repair(stream) data with other replica.
 *
 * When RemoteSyncTask receives SyncComplete from remote node, task completes.
 */
public class RemoteSyncTask extends SyncTask implements CompletableRemoteSyncTask
{
    private static final Logger logger = LoggerFactory.getLogger(RemoteSyncTask.class);

    public RemoteSyncTask(RepairJobDesc desc, InetAddressAndPort endpoint1, InetAddressAndPort endpoint2, List<Range<Token>> difference, PreviewKind previewKind)
    {
        super(desc, endpoint1, endpoint2, difference, previewKind);
    }

    @Override
    protected void startSync(List<Range<Token>> differences)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        SyncRequest request = new SyncRequest(desc, local, endpoint1, endpoint2, differences, previewKind);
        String message = String.format("Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", request.ranges.size(), request.src, request.dst);
        logger.info("{} {}", previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        MessagingService.instance().sendOneWay(request.createMessage(), request.src);
    }

    public void syncComplete(boolean success, List<SessionSummary> summaries)
    {
        if (success)
        {
            set(stat.withSummaries(summaries));
        }
        else
        {
            setException(new RepairException(desc, previewKind, String.format("Sync failed between %s and %s", endpoint1, endpoint2)));
        }
        finished();
    }

    @Override
    public NodePair nodes()
    {
        return new NodePair(endpoint1, endpoint2);
    }
}
