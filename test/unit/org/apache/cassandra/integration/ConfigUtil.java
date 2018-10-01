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


import java.beans.IntrospectionException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.GenericProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

public class ConfigUtil
{
    public static void writeConfigFile(File file, Config config) throws NoSuchFieldException, IntrospectionException
    {
        file.getParentFile().mkdirs();

        // need to output seed_provider and parameters as a singleton list (for some reason, this is the format necessary to parse it?)
        Representer representer = new Representer();
        Set<Property> configProperties = representer.getPropertyUtils().getProperties(Config.class);
        configProperties.remove(new FieldProperty(Config.class.getDeclaredField("seed_provider")));
        configProperties.add(new GenericProperty("seed_provider", List.class, List.class)
        {
            @Override
            public void set(Object o, Object o1) { throw new UnsupportedOperationException(); }
            @Override
            public Object get(Object o)
            {
                return ImmutableList.of(((Config)o).seed_provider);
            }
        });

        representer.addClassTag(ParameterizedClass.class, Tag.MAP);
        Set<Property> parameterizedClassProperties = representer.getPropertyUtils().getProperties(ParameterizedClass.class);
        parameterizedClassProperties.remove(new FieldProperty(ParameterizedClass.class.getDeclaredField("parameters")));
        parameterizedClassProperties.add(new GenericProperty("parameters", List.class, List.class)
        {
            @Override
            public void set(Object o, Object o1) { throw new UnsupportedOperationException(); }
            @Override
            public Object get(Object o)
            {
                return ImmutableList.of(((ParameterizedClass)o).parameters);
            }
        });

        Yaml yaml = new Yaml(representer);
        String text = yaml.dumpAs(config, Tag.YAML, DumperOptions.FlowStyle.AUTO);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file)))
        {
            bw.write(text);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Config defaultConfig()
    {
        try
        {
            String url = System.getProperty("cassandra.config");
            if (url == null)
                url = new File("conf/cassandra.yaml").toURI().toURL().toString();
            YamlConfigurationLoader loader = new YamlConfigurationLoader();
            return loader.loadConfig(new URL(url));
        }
        catch (MalformedURLException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static Config generateConfig(InstanceConfig instanceConfig)
    {
        Config config = defaultConfig();
        config.partitioner = instanceConfig.partitioner;
        config.broadcast_address = instanceConfig.broadcast_address;
        config.listen_address = instanceConfig.listen_address;
        config.broadcast_rpc_address = instanceConfig.broadcast_rpc_address;
        config.rpc_address = instanceConfig.rpc_address;
        config.saved_caches_directory = instanceConfig.saved_caches_directory;
        config.data_file_directories = instanceConfig.data_file_directories;
        config.commitlog_directory = instanceConfig.commitlog_directory;
        config.hints_directory = instanceConfig.hints_directory;
        config.concurrent_writes = instanceConfig.concurrent_writes;
        config.concurrent_counter_writes = instanceConfig.concurrent_counter_writes;
        config.concurrent_materialized_view_writes= instanceConfig.concurrent_materialized_view_writes;
        config.concurrent_reads = instanceConfig.concurrent_reads;
        config.memtable_flush_writers = instanceConfig.memtable_flush_writers;
        config.concurrent_compactors = instanceConfig.concurrent_compactors;
        config.memtable_heap_space_in_mb = instanceConfig.memtable_heap_space_in_mb;
        config.initial_token = instanceConfig.initial_token;
        return config;
    }
}
