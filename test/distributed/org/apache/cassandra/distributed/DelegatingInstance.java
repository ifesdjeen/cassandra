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

package org.apache.cassandra.distributed;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.ITestCluster;
import org.apache.cassandra.distributed.api.InstanceVersion;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.io.Serializable;
import java.util.UUID;

public class DelegatingInstance implements IInstance
{
    protected volatile IInstance delegate;
    public DelegatingInstance(IInstance delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public InetAddressAndPort getBroadcastAddress()
    {
        return delegate.getBroadcastAddress();
    }

    @Override
    public Object[][] executeInternal(String query, Object... args)
    {
        return delegate.executeInternal(query, args);
    }

    @Override
    public UUID getSchemaVersion()
    {
        return delegate.getSchemaVersion();
    }

    @Override
    public void schemaChange(String query)
    {
        delegate.schemaChange(query);
    }

    @Override
    public IInstanceConfig config()
    {
        return delegate.config();
    }

    @Override
    public void startup()
    {
        delegate.startup();
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }

    @Override
    public void setVersion(InstanceVersion version)
    {
        delegate.setVersion(version);
    }

    @Override
    public void startup(ITestCluster cluster)
    {
        delegate.startup(cluster);
    }

    @Override
    public void receiveMessage(Message message)
    {
        delegate.receiveMessage(message);
    }

    @Override
    public <T> SerializableCallable<T> callsOnInstance(SerializableCallable<T> call)
    {
        return delegate.callsOnInstance(call);
    }

    @Override
    public <T> T callOnInstance(SerializableCallable<T> call)
    {
        return delegate.callOnInstance(call);
    }

    @Override
    public SerializableRunnable runsOnInstance(SerializableRunnable run)
    {
        return delegate.runsOnInstance(run);
    }

    @Override
    public void runOnInstance(SerializableRunnable run)
    {
        delegate.runOnInstance(run);
    }

    @Override
    public <T> SerializableConsumer<T> acceptsOnInstance(SerializableConsumer<T> consumer)
    {
        return delegate.acceptsOnInstance(consumer);
    }

    @Override
    public <T1, T2> SerializableBiConsumer<T1, T2> acceptsOnInstance(SerializableBiConsumer<T1, T2> consumer)
    {
        return delegate.acceptsOnInstance(consumer);
    }

    @Override
    public <I, O> SerializableFunction<I, O> appliesOnInstance(SerializableFunction<I, O> f)
    {
        return delegate.appliesOnInstance(f);
    }

    @Override
    public <I1, I2, O> SerializableBiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f)
    {
        return delegate.appliesOnInstance(f);
    }

    @Override
    public <I1, I2, I3, O> SerializableTriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f)
    {
        return delegate.appliesOnInstance(f);
    }

    @Override
    public <E extends Serializable> E invokesOnInstance(E lambda)
    {
        return delegate.invokesOnInstance(lambda);
    }

    @Override
    public Object transferOneObject(Object object)
    {
        return delegate.transferOneObject(object);
    }
}
