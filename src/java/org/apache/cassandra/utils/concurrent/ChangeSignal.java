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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicLong;

public class ChangeSignal
{

    final WaitQueue queue = new WaitQueue();
    final AtomicLong changes = new AtomicLong();

    public class Monitor
    {
        private long seen;
        private Monitor(long change) { this.seen = seen; }

        private WaitQueue.Signal maybeQueue()
        {
            if (seen != changes.get())
                return null;

            WaitQueue.Signal signal = queue.register();
            if (seen != changes.get())
            {
                signal.cancel();
                return null;
            }
            return signal;
        }
        public void await() throws InterruptedException
        {
            WaitQueue.Signal signal;
            while ( null != (signal = maybeQueue()) )
                signal.await();
            seen = changes.get();
        }
        public boolean awaitUntil(long nanos) throws InterruptedException
        {
            WaitQueue.Signal signal = maybeQueue();
            if (signal == null || signal.awaitUntil(nanos))
            {
                seen = changes.get();
                return true;
            }
            return false;
        }
        public void awaitUninterruptibly()
        {
            WaitQueue.Signal signal = maybeQueue();
            if (signal != null)
                signal.awaitUninterruptibly();
            seen = changes.get();
        }
    }

    public Monitor monitor()
    {
        return new Monitor(changes.get());
    }

    public void signal()
    {
        changes.incrementAndGet();
        queue.signalAll();
    }

}
