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

package org.apache.cassandra.distributed.api;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface IInvokableInstance
{
    public interface SerializableCallable<T> extends Callable<T>, Serializable { public T call(); }
    public interface SerializableRunnable extends Runnable, Serializable {}
    public interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
    public interface SerializableBiConsumer<T1, T2> extends BiConsumer<T1, T2>, Serializable {}
    public interface SerializableFunction<I, O> extends Function<I, O>, Serializable {}
    public interface SerializableBiFunction<I1, I2, O> extends BiFunction<I1, I2, O>, Serializable {}
    public interface SerializableTriFunction<I1, I2, I3, O> extends Serializable { O apply(I1 i1, I2 i2, I3 i3); }
    public interface InstanceFunction<I, O> extends SerializableBiFunction {}

    <T> SerializableCallable<T> callsOnInstance(SerializableCallable<T> call);
    <T> T callOnInstance(SerializableCallable<T> call);

    SerializableRunnable runsOnInstance(SerializableRunnable run);
    void runOnInstance(SerializableRunnable run);

    <T> SerializableConsumer<T> acceptsOnInstance(SerializableConsumer<T> consumer);
    <T1, T2> SerializableBiConsumer<T1, T2> acceptsOnInstance(SerializableBiConsumer<T1, T2> consumer);

    <I, O> SerializableFunction<I, O> appliesOnInstance(SerializableFunction<I, O> f);
    <I1, I2, O> SerializableBiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f);
    <I1, I2, I3, O> SerializableTriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f);

    // E must be a functional interface, and lambda must be implemented by a lambda function
    <E extends Serializable> E invokesOnInstance(E lambda);

    Object transferOneObject(Object object);

}
