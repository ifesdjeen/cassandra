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

public interface MBeanWrapper
{
    static final Logger logger = LoggerFactory.getLogger(MBeanWrapper.class);

    static final MBeanWrapper instance = Boolean.getBoolean("org.apache.cassandra.disable_mbrean_registration") ?
                                         new NoOpMBeanWrapper() :
                                         new PlatformMBeanWrapper();

    public void registerMBean(Object obj, ObjectName mbeanName);
    public void registerMBean(Object obj, String mbeanName);
    public boolean isRegistered(ObjectName mbeanName);
    public boolean isRegistered(String mbeanName);
    public void unregisterMBean(ObjectName mbeanName);
    public void unregisterMBean(String mbeanName);

    static class NoOpMBeanWrapper implements MBeanWrapper
    {
        public void registerMBean(Object obj, ObjectName mbeanName) {}
        public void registerMBean(Object obj, String mbeanName) {}
        public boolean isRegistered(ObjectName mbeanName) { return false; }
        public boolean isRegistered(String mbeanName) { return false; }
        public void unregisterMBean(ObjectName mbeanName) {}
        public void unregisterMBean(String mbeanName) {}
    }

    static class PlatformMBeanWrapper implements MBeanWrapper
    {
        private final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        public void registerMBean(Object obj, ObjectName mbeanName)
        {
            try
            {
                mbs.registerMBean(obj, mbeanName);
            }
            catch (Exception e)
            {
                logger.error("Error registering MBean", e);
                throw new RuntimeException(e);
            }
        }

        public void registerMBean(Object obj, String mbeanName)
        {
            try
            {
                registerMBean(obj, new ObjectName(mbeanName));
            }
            catch (MalformedObjectNameException e)
            {
                logger.error("Error registering MBean", e);
                throw new RuntimeException(e);
            }
        }

        public boolean isRegistered(ObjectName mbeanName)
        {
            return mbs.isRegistered(mbeanName);
        }


        public boolean isRegistered(String mbeanName)
        {
            try
            {
                return isRegistered(new ObjectName(mbeanName));
            }
            catch (MalformedObjectNameException e)
            {
                logger.error("Error while checking if MBean is registered", e);
                throw new RuntimeException(e);
            }
        }

        public void unregisterMBean(ObjectName mbeanName)
        {
            try
            {
                mbs.unregisterMBean(mbeanName);
            }
            catch (Exception e)
            {
                logger.error("Error unregistering MBean", e);
                throw new RuntimeException(e);
            }
        }

        public void unregisterMBean(String mbeanName)
        {
            try
            {
                unregisterMBean(new ObjectName(mbeanName));
            }
            catch (MalformedObjectNameException e)
            {
                logger.error("Error registering MBean", e);
                throw new RuntimeException(e);
            }
        }
    }
}
