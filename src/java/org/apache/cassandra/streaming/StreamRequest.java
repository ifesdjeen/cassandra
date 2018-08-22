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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;

public class StreamRequest
{
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String keyspace;
    //Full replicas/transient replicas are not about the data to send, but about whether the requester
    //Is going to fully or transiently replicate that range ultimately.
    //This is necessary to disambiguate when enough data has been received for each range such as a fully replicated
    //range where it will some of the time need to fetch both full and transient ranges
    public final RangesAtEndpoint fullReplicas;
    public final RangesAtEndpoint transientReplicas;
    public final Collection<String> columnFamilies = new HashSet<>();

    public StreamRequest(String keyspace, RangesAtEndpoint fullReplicas, RangesAtEndpoint transientReplicas, Collection<String> columnFamilies)
    {
        this.keyspace = keyspace;
        this.fullReplicas = fullReplicas;
        this.transientReplicas = transientReplicas;
        this.columnFamilies.addAll(columnFamilies);
    }

    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeInt(request.columnFamilies.size());
            serializeReplicas(request.fullReplicas, out, version);
            serializeReplicas(request.transientReplicas, out, version);
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        private void serializeReplicas(RangesAtEndpoint replicas, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(replicas.size());
            for (Replica replica : replicas)
            {
                MessagingService.validatePartitioner(replica.range());
                CompactEndpointSerializationHelper.streamingInstance.serialize(replica.endpoint(), out, version);
                Token.serializer.serialize(replica.range().left, out, version);
                Token.serializer.serialize(replica.range().right, out, version);
                out.writeBoolean(replica.isFull());
            }
        }

        public StreamRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            int cfCount = in.readInt();
            RangesAtEndpoint fullReplicas = deserializeReplicas(in, version);
            RangesAtEndpoint transientReplicas = deserializeReplicas(in, version);
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, fullReplicas, transientReplicas, columnFamilies);
        }

        RangesAtEndpoint deserializeReplicas(DataInputPlus in, int version) throws IOException
        {
            int replicaCount = in.readInt();
            RangesAtEndpoint.Builder replicas = RangesAtEndpoint.builder(replicaCount);
            for (int i = 0; i < replicaCount; i++)
            {
                //TODO, super need to review the usage of streaming vs not streaming endpoint serialization helper
                //to make sure I'm not using the wrong one some of the time, like do repair messages use the
                //streaming version?
                // TODO: should we be serializing the endpoint here, given we guarantee only one unique endpoint?
                InetAddressAndPort endpoint = CompactEndpointSerializationHelper.streamingInstance.deserialize(in, version);
                Token left = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), version);
                Token right = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), version);
                boolean full = in.readBoolean();
                replicas.add(new Replica(endpoint, new Range<>(left, right), full));
            }
            return replicas.build();
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.sizeof(request.keyspace);
            size += TypeSizes.sizeof(request.columnFamilies.size());
            size += replicasSerializedSize(request.transientReplicas, version);
            size += replicasSerializedSize(request.fullReplicas, version);
            for (String cf : request.columnFamilies)
                size += TypeSizes.sizeof(cf);
            return size;
        }

        private long replicasSerializedSize(ReplicaCollection<?> replicas, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(replicas.size());
            for (Replica replica : replicas)
            {
                size += CompactEndpointSerializationHelper.streamingInstance.serializedSize(replica.endpoint(), version);
                size += Token.serializer.serializedSize(replica.range().left, version);
                size += Token.serializer.serializedSize(replica.range().right, version);
                size += TypeSizes.sizeof(replica.isFull());
            }
            return size;
        }

    }
}
