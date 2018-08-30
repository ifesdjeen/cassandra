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

import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;

public class TokenRanges implements Iterable<Range<Token>>
{
    public final ImmutableSet<Range<Token>> full;
    public final ImmutableSet<Range<Token>> trans;

    public static TokenRanges from(ReplicaCollection<?> replicas)
    {
        return new TokenRanges(replicas);
    }

    public static TokenRanges from(Collection<Range<Token>> full, Collection<Range<Token>> trans)
    {
        return new TokenRanges(full, trans);
    }

    private TokenRanges(Collection<Range<Token>> full, Collection<Range<Token>> trans)
    {
        this.full = ImmutableSet.copyOf(full);
        this.trans = ImmutableSet.copyOf(trans);
    }

    private TokenRanges(ReplicaCollection<?> replicas)
    {
        ImmutableSet.Builder<Range<Token>> fullBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<Range<Token>> transBuilder = ImmutableSet.builder();

        for (Replica endpoint : replicas)
        {
            if (endpoint.isFull())
                fullBuilder.add(endpoint.range());
            else
                transBuilder.add(endpoint.range());
        }

        this.full = fullBuilder.build();
        this.trans = fullBuilder.build();
    }

    @Override
    public Iterator<Range<Token>> iterator()
    {
        return Iterators.concat(full.iterator(), trans.iterator());
    }

    public boolean isFull(Range<Token> range)
    {
        if (full.contains(range))
            return true;

        assert trans.contains(range) : "Range is not present in this collection";
        return false;
    }

    public boolean isTransient(Range<Token> range)
    {
        if (trans.contains(range))
            return true;

        assert trans.contains(range) : "Range is not present in this collection";
        return false;
    }
}
