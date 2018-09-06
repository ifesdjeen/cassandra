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
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.Iterables.any;

public class DigestResolver<E extends Endpoints<E>, L extends ReplicaLayout<E, L>> extends ResponseResolver<E, L>
{
    private volatile MessageIn<ReadResponse> dataResponse;

    public DigestResolver(ReadCommand command, L replicas, long queryStartNanoTime)
    {
        super(command, replicas, queryStartNanoTime);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        Replica replica = replicaLayout.getReplicaFor(message.from);
        if (dataResponse == null && !message.payload.isDigestResponse() && replica.isFull())
        {
            dataResponse = message;
        }
        else if (replica.isTransient() && message.payload.isDigestResponse())
        {
            throw new IllegalStateException("digest response received from transient replica");
        }
    }

    @VisibleForTesting
    public boolean hasTransientResponse()
    {
        return hasTransientResponse(responses.snapshot());
    }

    private boolean hasTransientResponse(Collection<MessageIn<ReadResponse>> responses)
    {
        return any(responses,
                msg -> !msg.payload.isDigestResponse()
                        && replicaLayout.getReplicaFor(msg.from).isTransient());
    }

    public PartitionIterator getData()
    {
        assert isDataPresent();

        Collection<MessageIn<ReadResponse>> responses = this.responses.snapshot();

        if (!hasTransientResponse(responses))
        {
            return UnfilteredPartitionIterators.filter(dataResponse.payload.makeIterator(command), command.nowInSec());
        }
        else
        {
            // This path can be triggered only if we've got responses from full replicas and they match, but
            // transient replica response still contains data, which needs to be reconciled.
            DataResolver<E, L> dataResolver = new DataResolver<>(command,
                                                                 replicaLayout,
                                                                 UnfilteredPartitionIterators.MergeListener.NOOP,
                                                                 queryStartNanoTime);

            dataResolver.preprocess(dataResponse);
            // Reconcile with transient replicas
            for (MessageIn<ReadResponse> response : responses)
            {
                Replica replica = replicaLayout.getReplicaFor(response.from);
                if (replica.isTransient())
                    dataResolver.preprocess(response);
            }

            return dataResolver.resolve();
        }
    }

    public boolean responsesMatch()
    {
        long start = System.nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses.snapshot())
        {
            if (replicaLayout.getReplicaFor(message.from).isTransient())
                continue;

            ByteBuffer newDigest = message.payload.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return true;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }

    public DigestResolverDebugResult[] getDigestsByEndpoint()
    {
        DigestResolverDebugResult[] ret = new DigestResolverDebugResult[responses.size()];
        for (int i = 0; i < responses.size(); i++)
        {
            MessageIn<ReadResponse> message = responses.get(i);
            ReadResponse response = message.payload;
            String digestHex = ByteBufferUtil.bytesToHex(response.digest(command));
            ret[i] = new DigestResolverDebugResult(message.from, digestHex, message.payload.isDigestResponse());
        }
        return ret;
    }

    public static class DigestResolverDebugResult
    {
        public InetAddressAndPort from;
        public String digestHex;
        public boolean isDigestResponse;

        private DigestResolverDebugResult(InetAddressAndPort from, String digestHex, boolean isDigestResponse)
        {
            this.from = from;
            this.digestHex = digestHex;
            this.isDigestResponse = isDigestResponse;
        }
    }
}
