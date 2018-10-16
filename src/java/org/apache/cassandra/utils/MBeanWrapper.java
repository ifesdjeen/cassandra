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

package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to avoid catching and rethrowing checked exceptions on MBean and
 * allow turning of MBean registration for test purposes.
 */
public interface MBeanWrapper
{
    static final Logger logger = LoggerFactory.getLogger(MBeanWrapper.class);

    static final MBeanWrapper instance = Boolean.getBoolean("org.apache.cassandra.disable_mbrean_registration") ?
                                         new NoOpMBeanWrapper() :
                                         new PlatformMBeanWrapper();

    // Passing true for graceful will log exceptions instead of rethrowing them
    public void registerMBean(Object obj, ObjectName mbeanName, boolean graceful);
    default void registerMBean(Object obj, ObjectName mbeanName)
    {
        registerMBean(obj, mbeanName, false);
    }

    public void registerMBean(Object obj, String mbeanName, boolean graceful);
    default void registerMBean(Object obj, String mbeanName)
    {
        registerMBean(obj, mbeanName, false);
    }

    public boolean isRegistered(ObjectName mbeanName, boolean graceful);
    default boolean isRegistered(ObjectName mbeanName)
    {
        return isRegistered(mbeanName, false);
    }

    public boolean isRegistered(String mbeanName, boolean graceful);
    default boolean isRegistered(String mbeanName)
    {
        return isRegistered(mbeanName, false);
    }

    public void unregisterMBean(ObjectName mbeanName, boolean graceful);
    default void unregisterMBean(ObjectName mbeanName)
    {
        unregisterMBean(mbeanName, false);
    }

    public void unregisterMBean(String mbeanName, boolean graceful);
    default void unregisterMBean(String mbeanName)
    {
        unregisterMBean(mbeanName, false);
    }

    static class NoOpMBeanWrapper implements MBeanWrapper
    {
        public void registerMBean(Object obj, ObjectName mbeanName, boolean graceful) {}
        public void registerMBean(Object obj, String mbeanName, boolean graceful) {}
        public boolean isRegistered(ObjectName mbeanName, boolean graceful) { return false; }
        public boolean isRegistered(String mbeanName, boolean graceful) { return false; }
        public void unregisterMBean(ObjectName mbeanName, boolean graceful) {}
        public void unregisterMBean(String mbeanName, boolean graceful) {}
    }

    static class PlatformMBeanWrapper implements MBeanWrapper
    {
        private final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        public void registerMBean(Object obj, ObjectName mbeanName, boolean graceful)
        {
            try
            {
                mbs.registerMBean(obj, mbeanName);
            }
            catch (Exception e)
            {
                if (graceful)
                    logger.error("Error while registering MBean", e);
                else
                    throw new RuntimeException(e);
            }
        }

        public void registerMBean(Object obj, String mbeanName, boolean graceful)
        {
            try
            {
                mbs.registerMBean(obj, new ObjectName(mbeanName));
            }
            catch (Exception e)
            {
                if (graceful)
                    logger.error("Error while registering MBean", e);
                else
                    throw new RuntimeException(e);
            }
        }

        public boolean isRegistered(ObjectName mbeanName, boolean graceful)
        {
            try
            {
                return mbs.isRegistered(mbeanName);
            }
            catch (Exception e)
            {
                if (graceful)
                    logger.error("Error while checking if MBean is registered", e);
                else
                    throw new RuntimeException(e);
            }
            return false;
        }

        public boolean isRegistered(String mbeanName, boolean graceful)
        {
            try
            {
                return mbs.isRegistered(new ObjectName(mbeanName));
            }
            catch (Exception e)
            {
                if (graceful)
                    logger.error("Error while checking if MBean is registered", e);
                else
                    throw new RuntimeException(e);
            }
            return false;
        }

        public void unregisterMBean(ObjectName mbeanName, boolean graceful)
        {
            try
            {
                mbs.unregisterMBean(mbeanName);
            }
            catch (Exception e)
            {
                if (graceful)
                    logger.error("Error while unregistering MBean", e);
                else
                    throw new RuntimeException(e);
            }
        }

        public void unregisterMBean(String mbeanName, boolean graceful)
        {
            try
            {
                mbs.unregisterMBean(new ObjectName(mbeanName));
            }
            catch (Exception e)
            {
                if (graceful)
                    logger.error("Error while unregistering MBean", e);
                else
                    throw new RuntimeException(e);
            }
        }
    }
}
