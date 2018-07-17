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
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.reads.repair.ReadRepair;

public class DigestResolver extends ResponseResolver
{
    private volatile MessageIn<ReadResponse> dataResponse;
    private volatile boolean hasTransientResponse = false;
    private volatile TransientResolver transientResolver = null;

    public DigestResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, ReplicaCollection replicas, ReadRepair readRepair, int maxResponseCount, long queryStartNanoTime)
    {
        super(keyspace, command, consistency, replicas, readRepair, maxResponseCount, queryStartNanoTime);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        Replica replica = getReplicaFor(message.from);
        if (dataResponse == null && !message.payload.isDigestResponse() && replica.isFull())
        {
            dataResponse = message;
        }
        else if (replica.isTransient())
        {
            Preconditions.checkArgument(!message.payload.isDigestResponse(), "digest response received from transient replica");
            hasTransientResponse = true;
        }
    }

    public PartitionIterator getData()
    {
        assert isDataPresent();
        int nowInSec = command.nowInSec();

        if (transientResolver == null)
        {
            return UnfilteredPartitionIterators.filter(dataResponse.payload.makeIterator(command), nowInSec);
        }
        else
        {
            return transientResolver.resolve();
        }
    }

    public boolean responsesMatch()
    {
        long start = System.nanoTime();

        TransientResolver resolver = hasTransientResponse
                                              ? new TransientResolver(getReplicaFor(dataResponse.from), this)
                                              : null;

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            if (resolver != null)
                resolver.addResponse(message);

            ReadResponse response = message.payload;

            if (getReplicaFor(message.from).isTransient())
                continue;

            ByteBuffer newDigest = response.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        transientResolver = resolver;
        return true;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }

    @VisibleForTesting
    public boolean hasTransientResponse()
    {
        return hasTransientResponse;
    }
}
