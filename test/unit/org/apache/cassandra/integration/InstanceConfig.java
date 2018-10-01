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

import java.beans.IntrospectionException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class InstanceConfig implements Serializable
{
    final int num;
    final UUID hostId =java.util.UUID.randomUUID();
    final String partitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    final String broadcast_address;
    final String listen_address;
    final String broadcast_rpc_address;
    final String rpc_address;
    final String saved_caches_directory;
    final String[] data_file_directories;
    final String commitlog_directory;
    final String hints_directory;
    final int concurrent_writes = 2;
    final int concurrent_counter_writes = 2;
    final int concurrent_materialized_view_writes = 2;
    final int concurrent_reads = 2;
    final int memtable_flush_writers = 1;
    final int concurrent_compactors = 1;
    final int memtable_heap_space_in_mb = 10;
    final String initial_token;

    private InstanceConfig(int num,
                           String broadcast_address,
                           String listen_address,
                           String broadcast_rpc_address,
                           String rpc_address,
                           String saved_caches_directory,
                           String[] data_file_directories,
                           String commitlog_directory,
                           String hints_directory,
                           String initial_token)
    {
        this.num = num;
        this.broadcast_address = broadcast_address;
        this.listen_address = listen_address;
        this.broadcast_rpc_address = broadcast_rpc_address;
        this.rpc_address = rpc_address;
        this.saved_caches_directory = saved_caches_directory;
        this.data_file_directories = data_file_directories;
        this.commitlog_directory = commitlog_directory;
        this.hints_directory = hints_directory;
        this.initial_token = initial_token;
    }

    public static InstanceConfig generate(int nodeNum, File root, String token)
    {
        return new InstanceConfig(nodeNum,
                                  "127.0.0." + nodeNum,
                                  "127.0.0." + nodeNum,
                                  "127.0.0." + nodeNum,
                                  "127.0.0." + nodeNum,
                                  String.format("%s/node%d/saved_caches", root, nodeNum),
                                  new String[] { String.format("%s/node%d/data", root, nodeNum) },
                                  String.format("%s/node%d/commitlog", root, nodeNum),
                                  String.format("%s/node%d/hints", root, nodeNum),
                                  token);
    }

    private static Config defaultConfig()
    {
        try
        {
            String url = new File("test/conf/cassandra.yaml").toURI().toURL().toString();
            YamlConfigurationLoader loader = new YamlConfigurationLoader();
            return loader.loadConfig(new URL(url));
        }
        catch (MalformedURLException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public Config toCassandraConfig()
    {
        Config config = defaultConfig();
        config.partitioner = partitioner;
        config.broadcast_address = broadcast_address;
        config.listen_address = listen_address;
        config.broadcast_rpc_address = broadcast_rpc_address;
        config.rpc_address = rpc_address;
        config.saved_caches_directory = saved_caches_directory;
        config.data_file_directories = data_file_directories;
        config.commitlog_directory = commitlog_directory;
        config.hints_directory = hints_directory;
        config.concurrent_writes = concurrent_writes;
        config.concurrent_counter_writes = concurrent_counter_writes;
        config.concurrent_materialized_view_writes= concurrent_materialized_view_writes;
        config.concurrent_reads = concurrent_reads;
        config.memtable_flush_writers = memtable_flush_writers;
        config.concurrent_compactors = concurrent_compactors;
        config.memtable_heap_space_in_mb = memtable_heap_space_in_mb;
        config.initial_token = initial_token;
        return config;
    }

    public void writeCassandraConfigFile(File file) throws NoSuchFieldException, IntrospectionException
    {
        writeCassandraConfigFile(file, toCassandraConfig());
    }

    public static void writeCassandraConfigFile(File file, Config config) throws NoSuchFieldException, IntrospectionException
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

}
