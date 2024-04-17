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

package org.apache.cassandra.tcm;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.concurrent.Future;

/**
 * When debouncing from a replica we know exactly which epoch we need, so to avoid retries we
 * keep track of which epoch we are currently debouncing, and if a request for a newer epoch
 * comes in, we create a new future. If a request for a newer epoch comes in, we simply
 * swap out the current future reference for a new one which is requesting the newer epoch.
 */
public class EpochAwareDebounce
{
    private static final Logger logger = LoggerFactory.getLogger(EpochAwareDebounce.class);
    public static final EpochAwareDebounce instance = new EpochAwareDebounce();
    private final AtomicReference<EpochAwareFuture> currentFuture = new AtomicReference<>();

    private EpochAwareDebounce()
    {
    }

    /**
     * Deduplicate requests to catch up log state based on the desired epoch. Callers supply a target epoch and
     * a function obtain the ClusterMetadata that corresponds with it. It is expected that this function will make rpc
     * calls to peers, retrieving a LogState which can be applied locally to produce the necessary {@code
     * ClusterMetadata}. The function takes a {@code Promise<LogState>} as input, with the expectation that this
     * specific instance will be used to provide blocking behaviour when making the rpc calls that fetch the {@code
     * LogState}. These promises are memoized in order to cancel them when {@link #shutdownAndWait(long, TimeUnit)} is
     * called. This causes the fetch function to stop waiting on any in flight {@code LogState} requests and prevents
     * shutdown from being blocked.
     *
     * @param  fetchFunction executes the request for LogState. It's expected that this popluates fetchResult with the
     *                       successful result.
     * @param epoch the desired epoch
     * @return
     */
    public Future<ClusterMetadata> getAsync(Supplier<Future<ClusterMetadata>> fetchFunction,
                                            Epoch epoch)
    {
        while (true)
        {
            EpochAwareFuture running = currentFuture.get();
            if (running != null && !running.future.isDone() && running.epoch.isEqualOrAfter(epoch))
                return running.future;

            if (running == SENTINEL)
                continue;

            if (currentFuture.compareAndSet(running, SENTINEL))
            {
                Future<ClusterMetadata> promise = fetchFunction.get();
                boolean res = currentFuture.compareAndSet(SENTINEL, new EpochAwareFuture(epoch, promise));
                assert res : "Should not have happened";
                return promise;
            }
        }
    }

    private static final EpochAwareFuture SENTINEL = new EpochAwareFuture(Epoch.EMPTY, null);
    private static class EpochAwareFuture
    {
        private final Epoch epoch;
        private final Future<ClusterMetadata> future;
        public EpochAwareFuture(Epoch epoch, Future<ClusterMetadata> future)
        {
            this.epoch = epoch;
            this.future = future;
        }

    }

    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        // TODO: remove?
    }
}
