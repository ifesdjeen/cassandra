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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

@VisibleForTesting
public class RangeRelocator
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    private final StreamPlan streamPlan = new StreamPlan(StreamOperation.RELOCATION);
    private final InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
    private final TokenMetadata tokenMetaCloneAllSettled;
    // clone to avoid concurrent modification in calculateNaturalReplicas
    private final TokenMetadata tokenMetaClone;
    private final Collection<Token> tokens;
    private final List<String> keyspaceNames;


    RangeRelocator(Collection<Token> tokens, List<String> keyspaceNames, TokenMetadata tmd)
    {
        this.tokens = tokens;
        this.keyspaceNames = keyspaceNames;
        this.tokenMetaCloneAllSettled = tmd.cloneAfterAllSettled();
        // clone to avoid concurrent modification in calculateNaturalReplicas
        this.tokenMetaClone = tmd.cloneOnlyTokenMap();
    }

    @VisibleForTesting
    public RangeRelocator()
    {
        this.tokens = null;
        this.keyspaceNames = null;
        this.tokenMetaCloneAllSettled = null;
        this.tokenMetaClone = null;
    }

    /**
     * Wrapper that supplies accessors to the real implementations of the various dependencies for this method
     */
    private static Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> calculateRangesToFetchWithPreferredEndpoints(RangesAtEndpoint fetchRanges,
                                                                                                                         AbstractReplicationStrategy strategy,
                                                                                                                         String keyspace,
                                                                                                                         TokenMetadata tmdBefore,
                                                                                                                         TokenMetadata tmdAfter)
    {
        EndpointsByReplica preferredEndpoints =
        RangeStreamer.calculateRangesToFetchWithPreferredEndpoints(DatabaseDescriptor.getEndpointSnitch()::sortedByProximity,
                                                                   strategy,
                                                                   fetchRanges,
                                                                   StorageService.useStrictConsistency,
                                                                   tmdBefore,
                                                                   tmdAfter,
                                                                   RangeStreamer.ALIVE_PREDICATE,
                                                                   keyspace,
                                                                   Collections.emptyList());
        return RangeStreamer.convertPreferredEndpointsToWorkMap(preferredEndpoints);
    }

    /**
     * calculating endpoints to stream current ranges to if needed
     * in some situations node will handle current ranges as part of the new ranges
     **/
    public static RangesByEndpoint calculateRangesToStreamWithEndpoints(RangesAtEndpoint streamRanges,
                                                                        AbstractReplicationStrategy strat,
                                                                        TokenMetadata tmdBefore,
                                                                        TokenMetadata tmdAfter)
    {
        RangesByEndpoint.Mutable endpointRanges = new RangesByEndpoint.Mutable();
        for (Replica toStream : streamRanges)
        {
            //If the range we are sending is full only send it to the new full replica
            //There will also be a new transient replica we need to send the data to, but not
            //the repaired data
            EndpointsForRange currentEndpoints = strat.calculateNaturalReplicas(toStream.range().right, tmdBefore);
            EndpointsForRange newEndpoints = strat.calculateNaturalReplicas(toStream.range().right, tmdAfter);
            logger.debug("Need to stream {}, current endpoints {}, new endpoints {}", toStream, currentEndpoints, newEndpoints);

            outer:
            for (Replica updated : newEndpoints)
            {
                Set<Range<Token>> subsToStream = Collections.singleton(toStream.range());

                for (Replica current : currentEndpoints)
                {
                    // if the range isn't compeltely new for the endpoint
                    if (current.endpoint().equals(updated.endpoint()))
                    {
                        //Nothing to do
                        if (current.equals(updated))
                            continue outer;

                        //First subtract what we already have
                        if (current.isFull() == updated.isFull() || current.isFull())
                            subsToStream = toStream.range().subtract(current.range());

                        //Now we only stream what is still replicated
                        subsToStream = subsToStream.stream().flatMap(range -> range.intersectionWith(updated.range()).stream()).collect(Collectors.toSet());
                    }
                }

                for (Range<Token> tokenRange : subsToStream)
                {
                    endpointRanges.put(updated.endpoint(), updated.decorateSubrange(tokenRange));
                }
            }
        }
        return endpointRanges.asImmutableView();
    }

    public void calculateToFromStreams()
    {
        logger.debug("Current tmd " + tokenMetaClone);
        logger.debug("Updated tmd " + tokenMetaCloneAllSettled);
        for (String keyspace : keyspaceNames)
        {
            // replication strategy of the current keyspace
            AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
            // getting collection of the currently used ranges by this keyspace
            RangesAtEndpoint currentReplicas = strategy.getAddressReplicas(localAddress);

            logger.info("Calculating ranges to stream and request for keyspace {}", keyspace);
            //From what I have seen we only ever call this with a single token from StorageService.move(Token)
            for (Token newToken : tokens)
            {
                Collection<Token> currentTokens = tokenMetaClone.getTokens(localAddress);
                if (currentTokens.size() > 1 || currentTokens.isEmpty())
                {
                    throw new AssertionError("Unexpected current tokens: " + currentTokens);
                }

                // collection of ranges which this node will serve after move to the new token
                RangesAtEndpoint updatedReplicas = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);

                // calculated parts of the ranges to request/stream from/to nodes in the ring
                Pair<RangesAtEndpoint, RangesAtEndpoint> streamAndFetchOwnRanges = Pair.create(RangesAtEndpoint.empty(localAddress), RangesAtEndpoint.empty(localAddress));
                //In the single node token move there is nothing to do and Range subtraction is broken
                //so it's easier to just identify this case up front.
                if (tokenMetaClone.getTopology().getDatacenterEndpoints().get(DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort()
)).size() > 1)
                {
                    streamAndFetchOwnRanges = StorageService.calculateStreamAndFetchRanges(currentReplicas, updatedReplicas);
                }

                Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> workMap = calculateRangesToFetchWithPreferredEndpoints(streamAndFetchOwnRanges.right, strategy, keyspace, tokenMetaClone, tokenMetaCloneAllSettled);

                RangesByEndpoint endpointRanges = calculateRangesToStreamWithEndpoints(streamAndFetchOwnRanges.left, strategy, tokenMetaClone, tokenMetaCloneAllSettled);

                logger.info("Endpoint ranges to stream to " + endpointRanges);

                // stream ranges
                for (InetAddressAndPort address : endpointRanges.keySet())
                {
                    logger.debug("Will stream range {} of keyspace {} to endpoint {}", endpointRanges.get(address), keyspace, address);
                    RangesAtEndpoint ranges = endpointRanges.get(address);
                    streamPlan.transferRanges(address, keyspace, ranges);
                }

                // stream requests
                workMap.asMap().forEach((address, sourceAndOurReplicas) -> {
                    RangesAtEndpoint full = sourceAndOurReplicas.stream()
                            .filter(pair -> pair.remote.isFull())
                            .map(pair -> pair.local)
                            .collect(RangesAtEndpoint.collector(localAddress));
                    RangesAtEndpoint transientReplicas = sourceAndOurReplicas.stream()
                            .filter(pair -> pair.remote.isTransient())
                            .map(pair -> pair.local)
                            .collect(RangesAtEndpoint.collector(localAddress));
                    logger.debug("Will request range {} of keyspace {} from endpoint {}", workMap.get(address), keyspace, address);
                    streamPlan.requestRanges(address, keyspace, full, transientReplicas);
                });

                logger.debug("Keyspace {}: work map {}.", keyspace, workMap);
            }
        }
    }

    public Future<StreamState> stream()
    {
        return streamPlan.execute();
    }

    public boolean streamsNeeded()
    {
        return !streamPlan.isEmpty();
    }
}
