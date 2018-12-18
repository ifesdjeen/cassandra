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

import com.google.common.base.Predicate;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class InstanceClassLoader extends URLClassLoader
{
    // Classes that have to be shared between instances, for configuration or returning values
    private final static Set<String> sharedClassNames = Arrays.stream(new Class[]
            {
                    Pair.class,
                    InstanceConfig.class,
                    Message.class,
                    InetAddressAndPort.class,
                    ParameterizedClass.class,
                    InvokableInstance.SerializableCallable.class,
                    InvokableInstance.SerializableRunnable.class,
                    InvokableInstance.SerializableConsumer.class,
                    InvokableInstance.SerializableBiConsumer.class,
                    InvokableInstance.SerializableFunction.class,
                    InvokableInstance.SerializableBiFunction.class,
                    InvokableInstance.SerializableTriFunction.class,
                    InvokableInstance.InstanceFunction.class
            })
            .map(Class::getName)
            .collect(Collectors.toSet());

    private static final Predicate<String> isolatedPackageNames = name ->
            name.startsWith("org.slf4j") ||
            name.startsWith("ch.qos.logback") ||
            name.startsWith("org.yaml") ||
            name.startsWith("org.apache.cassandra.");

    private static final Predicate<String> sharedPackageNames = name ->
            name.startsWith("org.apache.cassandra.distributed.api.")
            || !isolatedPackageNames.apply(name);

    private static final Predicate<String> isSharedClass = name ->
            sharedPackageNames.apply(name) || sharedClassNames.contains(name);

    private final int id; // for debug purposes
    private final ClassLoader sharedClassLoader;
    private final Predicate<String> isSharedClassName;

    InstanceClassLoader(int id, URL[] urls, Predicate<String> isSharedClassName, ClassLoader sharedClassLoader)
    {
        super(urls, null);
        this.id = id;
        this.sharedClassLoader = sharedClassLoader;
        this.isSharedClassName = isSharedClassName;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        // Do not share:
        //  * yaml, which  is a rare exception because it does mess with loading org.cassandra...Config class instances
        //  * most of the rest of Cassandra classes (unless they were explicitly shared) g
        if (isSharedClassName.apply(name))
            return sharedClassLoader.loadClass(name);

        return loadClassInternal(name);
    }

    Class<?> loadClassInternal(String name) throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name))
        {
            // First, check if the class has already been loaded
            Class<?> c = findLoadedClass(name);

            if (c == null)
                c = findClass(name);

            return c;
        }
    }

    public static IntFunction<ClassLoader> createFactory(URLClassLoader contextClassLoader)
    {
        URL[] urls = contextClassLoader.getURLs();
        return id -> new InstanceClassLoader(id, urls, isSharedClass, contextClassLoader);
    }

}
