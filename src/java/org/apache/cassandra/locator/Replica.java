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

package org.apache.cassandra.locator;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A Replica is an endpoint, a token range that endpoint replicates (or used to replicate) and whether it replicates
 * that range fully or transiently. Generally it's a bad idea to pass around ranges when code depends on the transientness
 * of the replication. That means you should avoid unwrapping and rewrapping these things and think hard about subtraction
 * and such and what the result is WRT to transientness.
 *
 * Definitely avoid creating fake Replicas with misinformation about endpoints, ranges, or transientness.
 */
public class Replica implements Comparable<Replica>
{
    private final Range<Token> range;
    private final InetAddressAndPort endpoint;
    private final boolean full;

    public Replica(InetAddressAndPort endpoint, Range<Token> range, boolean full)
    {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(range);
        this.endpoint = endpoint;
        this.range = range;
        this.full = full;
    }

    public Replica(InetAddressAndPort endpoint, Token start, Token end, boolean full)
    {
        this(endpoint, new Range<>(start, end), full);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica replica = (Replica) o;
        return full == replica.full &&
               Objects.equals(endpoint, replica.endpoint) &&
               Objects.equals(range, replica.range);
    }

    public int hashCode()
    {
        return Objects.hash(endpoint, range, full);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(full ? "Full" : "Transient");
        sb.append('(').append(endpoint()).append(',').append(range).append(')');
        return sb.toString();
    }

    public final InetAddressAndPort endpoint()
    {
        return endpoint;
    }

    public boolean isLocal()
    {
        return endpoint.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    public Range<Token> range()
    {
        return range;
    }

    public boolean isFull()
    {
        return full;
    }

    public final boolean isTransient()
    {
        return !isFull();
    }

    /**
     * This is used exclusively in TokenMetadata to check if a portion of a range is already replicated
     * by an endpoint so that we only mark as pending the portion that is either not replicated sufficiently (transient
     * when we need full) or at all.
     *
     * If it's not replicated at all it needs to be pending because there is no data.
     * If it's replicated but only transiently and we need to replicate it fully it must be marked as pending until it
     * is available fully otherwise a read might treat this replica as full and not read from a full replica that has
     * the data.
     */
    public RangesAtEndpoint subtractByRange(RangesAtEndpoint toSubtract)
    {
        // TODO: is it OK to ignore transient status here?
        Set<Range<Token>> subtractedRanges = range().subtractAll(toSubtract.ranges());
        RangesAtEndpoint.Builder result = RangesAtEndpoint.builder(endpoint, subtractedRanges.size());
        for (Range<Token> range : subtractedRanges)
        {
            result.add(decorateSubrange(range));
        }
        return result.build();
    }

    /**
     * Don't use this method and ignore transient status unless you are explicitly handling it outside this method.
     *
     * This helper method is used by StorageService.calculateStreamAndFetchRanges to perform subtraction.
     * It ignores transient status because it's already being handled in calculateStreamAndFetchRanges.
     */
    public RangesAtEndpoint subtractIgnoreTransientStatus(Range<Token> subtract)
    {
        Set<Range<Token>> ranges = this.range.subtract(subtract);
        RangesAtEndpoint.Builder result = RangesAtEndpoint.builder(endpoint, ranges.size());
        for (Range<Token> subrange : ranges)
            result.add(decorateSubrange(subrange));
        return result.build();
    }

    public boolean contains(Range<Token> that)
    {
        return range().contains(that);
    }

    public boolean intersectsOnRange(Replica replica)
    {
        return range().intersects(replica.range());
    }

    public Replica decorateSubrange(Range<Token> subrange)
    {
        Preconditions.checkArgument(range.contains(subrange));
        return new Replica(endpoint(), subrange, isFull());
    }

    public static Replica full(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, true);
    }

    public static Replica full(InetAddressAndPort endpoint, Token start, Token end)
    {
        return full(endpoint, new Range<>(start, end));
    }

    public static Replica trans(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, false);
    }

    public static Replica trans(InetAddressAndPort endpoint, Token start, Token end)
    {
        return trans(endpoint, new Range<>(start, end));
    }

    @Override
    public int compareTo(Replica o)
    {
        int c = range.compareTo(o.range);
        if (c == 0)
            c = endpoint.compareTo(o.endpoint);
        if (c == 0)
            c =  Boolean.compare(full, o.full);
        return c;
    }
}

