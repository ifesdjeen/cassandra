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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

public class VerbFilter implements BiFunction<InetAddressAndPort, String, Boolean>
{
    private Map<InetAddressAndPort, String> verbMap = new HashMap<>();

    public void dropMessagesFor(Object[] pairs)
    {
        if (pairs.length % 2 != 0)
            throw new IllegalArgumentException("Expected ip/verb pairs");

        for (int i = 0; i < pairs.length; i += 2)
        {
            InetAddressAndPort addr = (InetAddressAndPort) pairs[i];
            String verb = ((MessagingService.Verb) pairs[i + 1]).toString();
            verbMap.put(addr, verb);
        }
    }

    public Boolean apply(InetAddressAndPort addr, String verb)
    {
        return !verbMap.containsKey(addr) || !verbMap.get(addr).equals(verb);
    }

    public void reset()
    {
        verbMap.clear();
    }
}
