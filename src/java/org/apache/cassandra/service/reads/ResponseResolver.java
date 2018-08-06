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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.ReplicaPlan;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.concurrent.Accumulator;

public abstract class ResponseResolver
{
    protected static final Logger logger = LoggerFactory.getLogger(ResponseResolver.class);

    protected final Keyspace keyspace;
    protected final ReadCommand command;
    protected final ConsistencyLevel consistency;
    protected final ReplicaPlan replicaPlan;
    protected final ReadRepair readRepair;

    // Accumulator gives us non-blocking thread-safety with optimal algorithmic constraints
    protected final Accumulator<MessageIn<ReadResponse>> responses;
    protected final long queryStartNanoTime;

    public ResponseResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, ReplicaPlan replicaPlan, ReadRepair readRepair, long queryStartNanoTime)
    {
        this.keyspace = keyspace;
        this.command = command;
        this.consistency = consistency;
        this.replicaPlan = replicaPlan;
        this.readRepair = readRepair;
        this.responses = new Accumulator<>(replicaPlan.allReplicas().size());
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public abstract boolean isDataPresent();

    public void preprocess(MessageIn<ReadResponse> message)
    {
        try
        {
            responses.add(message);
        }
        catch (IllegalStateException e)
        {
            logger.error("Encountered error while trying to preprocess the message {}: %s in command {}, replicas: {}", message, command, readRepair, consistency, replicaPlan.targetReplicas());
            throw e;
        }
    }

    public Accumulator<MessageIn<ReadResponse>> getMessages()
    {
        return responses;
    }
}
