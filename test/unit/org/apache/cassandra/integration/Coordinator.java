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

package org.apache.cassandra.integration;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;


public class Coordinator
{
    private final TestCluster cluster;
    private final Instance instance;

    public Coordinator(TestCluster cluster, Instance instance)
    {
        this.cluster = cluster;
        this.instance = instance;
    }

    public void init()
    {
        instance.appliesOnInstance(Coordinator::registerCallbacks)
                .apply(cluster.appliesOnInstance(Instance::handleWrite),
                       cluster.appliesOnInstance(Instance::handleRead));
    }

    private static Object[][] doCoordinatorWrite(String query, int consistencyLevel)
    {
        CQLStatement prepared = QueryProcessor.getStatement(query, ClientState.forInternalCalls());
        assert prepared instanceof ModificationStatement;
        ModificationStatement modificationStatement = (ModificationStatement) prepared;

        modificationStatement.execute(QueryState.forInternalCalls(),
                                      QueryOptions.forInternalCalls(ConsistencyLevel.fromCode(consistencyLevel), Collections.emptyList()),
                                      System.nanoTime());

        return new Object[][] {};
    }

    private static Object[][] doCoordinatorRead(String query, int consistencyLevel)
    {
        CQLStatement prepared = QueryProcessor.getStatement(query, ClientState.forInternalCalls());
        assert prepared instanceof SelectStatement;
        SelectStatement selectStatement = (SelectStatement) prepared;
        ReadQuery readQuery = selectStatement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());

        PartitionIterator pi = readQuery.execute(ConsistencyLevel.fromCode(consistencyLevel), ClientState.forInternalCalls(), System.nanoTime());
        ResultMessage.Rows rows = selectStatement.processResults(pi, QueryOptions.DEFAULT, FBUtilities.nowInSeconds(), 10);
        return RowUtil.toObjects(rows);
    }

    public Object[][] coordinatorWrite(String query, int consistencyLevel)
    {
        return instance.appliesOnInstance(Coordinator::doCoordinatorWrite).apply(query, consistencyLevel);
    }

    public Object[][] coordinatorRead(String query, int consistencyLevel)
    {
        return instance.appliesOnInstance(Coordinator::doCoordinatorRead).apply(query, consistencyLevel);
    }

    public static Void registerCallbacks(BiFunction<InetAddressAndPort, ByteBuffer, ByteBuffer> writeHandler,
                                         BiFunction<InetAddressAndPort, ByteBuffer, ByteBuffer> readHandler)
    {
        MessagingService.instance().addMessageSink((new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
            {
                System.out.println("RECEIVED IN MSG: " + message);
                try
                {
                    DataOutputBuffer out = new DataOutputBuffer(1024);

                    MessageIn response;
                    if (message.verb == MessagingService.Verb.MUTATION ||
                        message.verb == MessagingService.Verb.READ_REPAIR)
                    {
                        MessageOut<Mutation> mutationMessage = (MessageOut<Mutation>) message;
                        Mutation.serializer.serialize(mutationMessage.payload, out, MessagingService.current_version);
                        ByteBuffer responseBB = writeHandler.apply(to, out.buffer());
                        DataInputBuffer dib = new DataInputBuffer(responseBB, false);
                        WriteResponse writeResponse = WriteResponse.serializer.deserialize(dib, MessagingService.current_version);

                        response = new MessageIn<>(to, writeResponse, Collections.emptyMap(),
                                                   MessagingService.Verb.REQUEST_RESPONSE,
                                                   MessagingService.current_version,
                                                   System.nanoTime());
                    }
                    else if (message.verb == MessagingService.Verb.READ)
                    {
                        MessageOut<ReadCommand> readCommandMessage = (MessageOut<ReadCommand>) message;
                        ReadCommand.serializer.serialize(readCommandMessage.payload, out, MessagingService.current_version);
                        ByteBuffer responseBB = readHandler.apply(to, out.buffer());
                        DataInputBuffer dib = new DataInputBuffer(responseBB, false);
                        ReadResponse readResponse = ReadResponse.serializer.deserialize(dib, MessagingService.current_version);

                        response = new MessageIn<>(to, readResponse, Collections.emptyMap(),
                                                   MessagingService.Verb.REQUEST_RESPONSE,
                                                   MessagingService.current_version,
                                                   System.nanoTime());
                    }
                    else
                    {
                        System.out.println("ERROR");
                        throw new RuntimeException("Do now know how to handle message: " + message);
                    }

                    MessagingService.instance().receive(response, id);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }

                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                System.out.println("Incoming Message: " + message);
                return true;
            }
        }));

        return null;
    }
}
