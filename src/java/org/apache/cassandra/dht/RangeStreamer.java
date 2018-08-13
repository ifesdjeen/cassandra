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
package org.apache.cassandra.dht;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;

import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;

/**
 * Assists in streaming ranges to this node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    public static Predicate<Replica> ALIVE_PREDICATE = replica ->
                                                             (!Gossiper.instance.isEnabled() ||
                                                              (Gossiper.instance.getEndpointStateForEndpoint(replica.endpoint()) == null ||
                                                               Gossiper.instance.getEndpointStateForEndpoint(replica.endpoint()).isAlive())) &&
                                                             FailureDetector.instance.isAlive(replica.endpoint());

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final InetAddressAndPort address;
    /* streaming description */
    private final String description;
    private final Multimap<String, Multimap<InetAddressAndPort, FetchReplica>> toFetch = HashMultimap.create();
    private final Set<Predicate<Replica>> sourceFilters = new HashSet<>();
    private final StreamPlan streamPlan;
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;

    public static class FetchReplica
    {
        public final Replica local;
        public final Replica remote;

        public FetchReplica(Replica local, Replica remote)
        {
            this.local = local;
            this.remote = remote;
        }
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements Predicate<Replica>
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        public boolean apply(Replica replica)
        {
            return fd.isAlive(replica.endpoint());
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements Predicate<Replica>
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean apply(Replica replica)
        {
            return snitch.getDatacenter(replica).equals(sourceDc);
        }
    }

    /**
     * Source filter which excludes the current node from source calculations
     */
    public static class ExcludeLocalNodeFilter implements Predicate<Replica>
    {
        public boolean apply(Replica replica)
        {
            return !replica.isLocal();
        }
    }

    /**
     * Source filter which only includes endpoints contained within a provided set.
     */
    public static class WhitelistedSourcesFilter implements Predicate<Replica>
    {
        private final Set<InetAddressAndPort> whitelistedSources;

        public WhitelistedSourcesFilter(Set<InetAddressAndPort> whitelistedSources)
        {
            this.whitelistedSources = whitelistedSources;
        }

        public boolean apply(Replica replica)
        {
            return whitelistedSources.contains(replica.endpoint());
        }
    }

    public RangeStreamer(TokenMetadata metadata,
                         Collection<Token> tokens,
                         InetAddressAndPort address,
                         StreamOperation streamOperation,
                         boolean useStrictConsistency,
                         IEndpointSnitch snitch,
                         StreamStateStore stateStore,
                         boolean connectSequentially,
                         int connectionsPerHost)
    {
        Preconditions.checkArgument(streamOperation == StreamOperation.BOOTSTRAP || streamOperation == StreamOperation.REBUILD, streamOperation);
        this.metadata = metadata;
        this.tokens = tokens;
        this.address = address;
        this.description = streamOperation.getDescription();
        this.streamPlan = new StreamPlan(streamOperation, connectionsPerHost, connectSequentially, null, PreviewKind.NONE);
        this.useStrictConsistency = useStrictConsistency;
        this.snitch = snitch;
        this.stateStore = stateStore;
        streamPlan.listeners(this.stateStore);
    }

    public void addSourceFilter(Predicate<Replica> filter)
    {
        sourceFilters.add(filter);
    }

    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param replicas ranges to be streamed
     */
    public void addRanges(String keyspaceName, ReplicaCollection<?> replicas)
    {
        if(Keyspace.open(keyspaceName).getReplicationStrategy() instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        boolean useStrictSource = useStrictSourcesForRanges(keyspaceName);
        EndpointsByReplica rangesForKeyspace = calculateRangesToFetchWithPreferredEndpoints(replicas, keyspaceName, useStrictSource);

        for (Map.Entry<Replica, Replica> entry : rangesForKeyspace.flattenEntries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);

        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();

        Multimap<InetAddressAndPort, FetchReplica> rangeFetchMap;
        //Only use the optimized strategy if we don't care about strict sources, have a replication factor > 1, and no
        //transient replicas.
        if (useStrictSource || strat == null || strat.getReplicationFactor().replicas == 1 || strat.getReplicationFactor().trans > 1)
        {
            rangeFetchMap = convertPreferredEndpointsToWorkMap(rangesForKeyspace);
        }
        else
        {
            rangeFetchMap = getOptimizedRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName);
        }

        toFetch.put(keyspaceName, rangeFetchMap);
        for (Map.Entry<InetAddressAndPort, Collection<FetchReplica>> entry : rangeFetchMap.asMap().entrySet())
        {
            if (logger.isTraceEnabled())
            {
                for (FetchReplica r : entry.getValue())
                    logger.trace("{}: range source {} local range {} for keyspace {}", description, r.remote, r.local, keyspaceName);
            }
        }
    }

    /**
     * @param keyspaceName keyspace name to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return useStrictConsistency
                && tokens != null
                && metadata.getSizeOfAllEndpoints() != strat.getReplicationFactor().replicas;
    }

    /**
     * Wrapper method to assemble the arguments for invoking the implementation with RangeStreamer's parameters
     * @param fetchRanges
     * @param keyspace
     * @param useStrictConsistency
     * @return
     */
    private EndpointsByReplica calculateRangesToFetchWithPreferredEndpoints(ReplicaCollection<?> fetchRanges, String keyspace, boolean useStrictConsistency)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();

        TokenMetadata tmd = metadata.cloneOnlyTokenMap();

        TokenMetadata tmdAfter = null;

        if (tokens != null)
        {
            // Pending ranges
            tmdAfter =  tmd.cloneOnlyTokenMap();
            tmdAfter.updateNormalTokens(tokens, address);
        }
        else if (useStrictConsistency)
        {
            throw new IllegalArgumentException("Can't ask for strict consistency and not supply tokens");
        }

        return RangeStreamer.calculateRangesToFetchWithPreferredEndpoints(snitch::sortedByProximity,
                                                                           strat,
                                                                           fetchRanges,
                                                                           useStrictConsistency,
                                                                           tmd,
                                                                           tmdAfter,
                                                                           ALIVE_PREDICATE,
                                                                           keyspace,
                                                                           sourceFilters);

    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     *
     **/
     public static EndpointsByReplica
     calculateRangesToFetchWithPreferredEndpoints(BiFunction<InetAddressAndPort, EndpointsForRange, EndpointsForRange> snitchGetSortedListByProximity,
                                                  AbstractReplicationStrategy strat,
                                                  ReplicaCollection<?> fetchRanges,
                                                  boolean useStrictConsistency,
                                                  TokenMetadata tmdBefore,
                                                  TokenMetadata tmdAfter,
                                                  Predicate<Replica> isAlive,
                                                  String keyspace,
                                                  Collection<Predicate<Replica>> sourceFilters)
    {
        EndpointsByRange rangeAddresses = strat.getRangeAddresses(tmdBefore);

        Predicate<Replica> isNotAlive = Predicates.not(isAlive);
        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
        logger.debug ("Keyspace: {}", keyspace);
        logger.debug("To fetch RN: {}", fetchRanges);
        logger.debug("Fetch ranges: {}", rangeAddresses);

        Predicate<Replica> sourceFiltersPredicate = Predicates.and(sourceFilters);

        //This list of replicas is just candidates. With strict consistency it's going to be a narrow list.
        EndpointsByReplica.Mutable rangesToFetchWithPreferredEndpoints = new EndpointsByReplica.Mutable();
        for (Replica toFetch : fetchRanges)
        {
            //Replica that is sufficient to provide the data we need
            //With strict consistency and transient replication we may end up with multiple types
            //so this isn't used with strict consistency
            Predicate<Replica> replicaIsSufficientFilter = toFetch.isFull() ? Replica::isFull : Predicates.alwaysTrue();
            logger.debug("To fetch {}", toFetch);
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(toFetch.range()))
                {
                    //Ultimately we populate this with whatever is going to be fetched from to satisfy toFetch
                    //It could be multiple endpoints and we must fetch from all of them if they are there
                    //With transient replication and strict consistency this is to get the full data from a full replica and
                    //transient data from the transient replica losing data
                    EndpointsForRange endpoints;
                    Predicate<Replica> notSelf = replica -> !replica.endpoint().equals(localAddress);
                    if (useStrictConsistency)
                    {
                        //Start with two sets of who replicates the range before and who replicates it after
                        EndpointsForRange oldEndpoints = rangeAddresses.get(range);
                        EndpointsForRange newEndpoints = strat.calculateNaturalReplicas(toFetch.range().right, tmdAfter);
                        logger.debug("Old endpoints {}", oldEndpoints);
                        logger.debug("New endpoints {}", newEndpoints);

                        //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                        //So we need to be careful to only be strict when endpoints == RF
                        if (oldEndpoints.size() == strat.getReplicationFactor().replicas)
                        {
                            Predicate<Replica> endpointNotReplicatedAnymore = replica -> !any(newEndpoints, newReplica -> newReplica.endpoint().equals(replica.endpoint()));
                            //Remove new endpoints from old endpoints based on address
                            oldEndpoints = oldEndpoints.filter(endpointNotReplicatedAnymore);

                            if (any(oldEndpoints, isNotAlive))
                                throw new IllegalStateException("A node required to move the data consistently is down: "
                                                                + oldEndpoints.filter(isNotAlive));

                            if (oldEndpoints.size() > 1)
                                throw new AssertionError("Expected <= 1 endpoint but found " + oldEndpoints);

                            //If we are transitioning from transient to full and and the set of replicas for the range is not changing
                            //we might end up with no endpoints to fetch from by address. In that case we can pick any full replica safely
                            //since we are already a transient replica and the existing replica remains.
                            //The old behavior where we might be asked to fetch ranges we don't need shouldn't occur anymore.
                            //So it's an error if we don't find what we need.
                            if (oldEndpoints.isEmpty() && toFetch.isTransient())
                            {
                                throw new AssertionError("If there are no endpoints to fetch from then we must be transitioning from transient to full for range " + toFetch);
                            }

                            //Need an additional full replica
                            if (toFetch.isFull() && !any(oldEndpoints, Replica::isFull))
                            {
                                Optional<Replica> fullReplica = Iterables.<Replica>tryFind(rangeAddresses.get(range), Predicates.and(Replica::isFull, notSelf, isAlive, sourceFiltersPredicate)).toJavaUtil();
                                if (fullReplica.isPresent())
                                    oldEndpoints = Endpoints.concat(oldEndpoints, EndpointsForRange.of(fullReplica.get()), false);
                                else
                                    throw new IllegalStateException("Couldn't find an alive full replica");
                            }
                        }
                        else
                        {
                            oldEndpoints = oldEndpoints.filter(Predicates.and(notSelf, isAlive, replicaIsSufficientFilter));
                        }

                        endpoints = oldEndpoints;

                        //We have to check the source filters here to see if they will remove any replicas
                        //required for strict consistency
                        if (!all(endpoints, Predicates.not(sourceFiltersPredicate)))
                        {
                            throw new IllegalStateException("Necessary replicas for strict consistency were removed by source filters: " + endpoints.filter(Predicates.not(sourceFiltersPredicate)));
                        }
                    }
                    else
                    {
                        //Without strict consistency we have given up on correctness so no point in fetching from
                        //a random full + transient replica since it's also likely to lose data
                        //TODO this is returning multiple replicas and we need to reduce it to just one
                        endpoints = snitchGetSortedListByProximity.apply(localAddress, rangeAddresses.get(range))
                                                                  .filter(Predicates.and(replicaIsSufficientFilter, notSelf, isAlive));
                    }

                    //Apply additional policy filters that were given to us, and establish everything remaining is alive for the strict case
                    endpoints = endpoints.filter(sourceFiltersPredicate);

                    // storing range and preferred endpoint set
                    rangesToFetchWithPreferredEndpoints.putAll(toFetch, endpoints, false);
                    logger.debug("Endpoints to fetch for {} are {}", toFetch, endpoints);
                }
            }

            EndpointsForRange addressList = rangesToFetchWithPreferredEndpoints.getIfPresent(toFetch);
            if (addressList == null)
                throw new IllegalStateException("Failed to find endpoints to fetch " + toFetch);

            /*
             * When we move forwards (shrink our bucket) we are the one losing a range and no one else loses
             * from that action (we also don't gain). When we move backwards there are two people losing a range. One is a full replica
             * and the other is a transient replica. So we must need fetch from two places in that case for the full range we gain.
             * For a transient range we only need to fetch from one.
             */
            if (useStrictConsistency && addressList.size() > 1 && (addressList.filter(Replica::isFull).size() > 1 || addressList.filter(Replica::isTransient).size() > 1))
                throw new IllegalStateException(String.format("Multiple strict sources found for %s, sources: %s", toFetch, addressList));

            //We must have enough stuff to fetch from
            if ((toFetch.isFull() && !any(addressList, Replica::isFull)) || addressList.isEmpty())
            {
                if (strat.getReplicationFactor().replicas == 1)
                {
                    if (useStrictConsistency)
                    {
                        logger.warn("A node required to move the data consistently is down");
                        throw new IllegalStateException("Unable to find sufficient sources for streaming range " + toFetch + " in keyspace " + keyspace + " with RF=1. " +
                                                        "Ensure this keyspace contains replicas in the source datacenter.");
                    }
                    else
                        logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                                    "Keyspace might be missing data.", toFetch, keyspace);

                }
                else
                {
                    if (useStrictConsistency)
                        logger.warn("A node required to move the data consistently is down");
                    throw new IllegalStateException("Unable to find sufficient sources for streaming range " + toFetch + " in keyspace " + keyspace);
                }
            }
        }
        return rangesToFetchWithPreferredEndpoints.asImmutableView();
    }

    /**
     * The preferred endpoint list is the wrong format because it is keyed by Replica (this node) rather than the source
     * endpoint we will fetch from which streaming wants.
     * @param preferredEndpoints
     * @return
     */
    public static Multimap<InetAddressAndPort, FetchReplica> convertPreferredEndpointsToWorkMap(EndpointsByReplica preferredEndpoints)
    {
        Multimap<InetAddressAndPort, FetchReplica> workMap = HashMultimap.create();
        for (Map.Entry<Replica, EndpointsForRange> e : preferredEndpoints.entrySet())
        {
            for (Replica source : e.getValue())
            {
                assert (e.getKey()).isLocal();
                assert !source.isLocal();
                workMap.put(source.endpoint(), new FetchReplica(e.getKey(), source));
            }
        }
        logger.debug("Work map {}", workMap);
        return workMap;
    }

    private static Multimap<InetAddressAndPort, FetchReplica> getOptimizedRangeFetchMap(EndpointsByReplica rangesWithSources,
                                                                                        Collection<Predicate<Replica>> sourceFilters, String keyspace)
    {
        //For now we just aren't going to use the optimized range fetch map with transient replication to shrink
        //the surface area to test and introduce bugs.
        //In the future it's possible we could run it twice once for full ranges with only full replicas
        //and once with transient ranges and all replicas. Then merge the result.
        EndpointsByRange.Mutable unwrapped = new EndpointsByRange.Mutable();
        for (Map.Entry<Replica, Replica> entry : rangesWithSources.flattenEntries())
        {
            Replicas.checkFull(entry.getValue());
            unwrapped.put(entry.getKey().range(), entry.getValue());
        }

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(unwrapped.asImmutableView(), sourceFilters, keyspace);
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = calculator.getRangeFetchMap();
        logger.info("Output from RangeFetchMapCalculator for keyspace {}", keyspace);
        validateRangeFetchMap(unwrapped.asImmutableView(), rangeFetchMapMap, keyspace);

        //Need to rewrap as Replicas
        Multimap<InetAddressAndPort, FetchReplica> wrapped = HashMultimap.create();
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            Replica toFetch = null;
            for (Replica r : rangesWithSources.keySet())
            {
                if (r.range().equals(entry.getValue()))
                {
                    if (toFetch != null)
                        throw new AssertionError(String.format("There shouldn't be multiple replicas for range %s, replica %s and %s here", r.range(), r, toFetch));
                    toFetch = r;
                }
            }
            if (toFetch == null)
                throw new AssertionError("Shouldn't be possible for the Replica we fetch to be null here");
            //Committing the cardinal sin of synthesizing a Replica, but it's ok because we assert earlier all of them
            //are full and optimized range fetch map doesn't support transient replication yet.
            wrapped.put(entry.getKey(), new FetchReplica(toFetch, Replica.full(entry.getKey(), entry.getValue())));
        }

        return wrapped;
    }

    /**
     * Verify that source returned for each range is correct
     * @param rangesWithSources
     * @param rangeFetchMapMap
     * @param keyspace
     */
    private static void validateRangeFetchMap(EndpointsByRange rangesWithSources, Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap, String keyspace)
    {
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            if(entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue()
                                        + " in keyspace " + keyspace);
            }

            if (!rangesWithSources.get(entry.getValue()).endpoints().contains(entry.getKey()))
            {
                throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue()
                                                + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
            }

            logger.info("Streaming range {} from endpoint {} for keyspace {}", entry.getValue(), entry.getKey(), keyspace);
        }
    }

    // For testing purposes
    @VisibleForTesting
    Multimap<String, Multimap<InetAddressAndPort, FetchReplica>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        toFetch.forEach((keyspace, sources) -> {
            logger.debug("Keyspace {} Sources {}", keyspace, sources);
            sources.asMap().forEach((source, fetchReplicas) -> {

                // filter out already streamed ranges
                RangesAtEndpoint available = stateStore.getAvailableRanges(keyspace, StorageService.instance.getTokenMetadata().partitioner);

                //It's a bit unpredictable as to whether we need to fetch a replica or not
                //because some of the time we will need it both fully and transiently and we only store what we already
                //have not what we need.
                //However at this point we have already calculated what we need so it doesn't matter it's not stored
                //we just need to check if we already fetched the data once the way we need it.
                Predicate<FetchReplica> isAvailable = fetch -> {
                    if (fetch.local.isFull() && fetch.remote.isFull())
                    {
                        return available.contains(fetch.local.range(), true);
                    }
                    else if (fetch.local.isFull() && fetch.remote.isTransient())
                    {
                        return available.contains(fetch.local.range(), false);
                    }
                    else if (fetch.local.isTransient())
                    {
                        return available.ranges().contains(fetch.local.range());
                    }
                    else
                    {
                        throw new AssertionError("Unreachable");
                    }
                };

                Iterator<FetchReplica> skipped = fetchReplicas.stream().filter(isAvailable).iterator();
                if (!skipped.hasNext())
                {
                    logger.info("Some ranges of {} are already available. Skipping streaming those ranges. Skipping {}. Fully available {} Transiently available {}",
                            fetchReplicas, Iterators.toString(skipped), available.filter(Replica::isFull).ranges(), available.filter(Replica::isTransient).ranges());
                }

                Iterable<FetchReplica> remaining = () -> fetchReplicas.stream().filter(not(isAvailable)).iterator();
                if (logger.isTraceEnabled())
                    logger.trace("{}ing from {} ranges {}", description, source, StringUtils.join(remaining, ", "));

                RangesAtEndpoint fullReplicas = fetchReplicas.stream()
                        .filter(pair -> pair.remote.isFull())
                        .map(pair -> pair.local)
                        .collect(RangesAtEndpoint.collector());
                RangesAtEndpoint transientReplicas = fetchReplicas.stream()
                        .filter(pair -> pair.remote.isTransient())
                        .map(pair -> pair.local)
                        .collect(RangesAtEndpoint.collector());

                logger.debug("Source and our replicas {}", fetchReplicas);
                logger.debug("Source {} Keyspace {}  streaming full {} transient {}", source, keyspace, fullReplicas, transientReplicas);

                /* Send messages to respective folks to stream data over to me */
                streamPlan.requestRanges(source, keyspace, fullReplicas, transientReplicas);
            });
        });

        return streamPlan.execute();
    }
}
