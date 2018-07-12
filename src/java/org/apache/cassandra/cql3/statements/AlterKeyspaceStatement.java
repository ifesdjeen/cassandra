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
package org.apache.cassandra.cql3.statements;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AlterKeyspaceStatement extends SchemaAlteringStatement
{
    private static final boolean allow_alter_rf_during_range_movement = Boolean.getBoolean(Config.PROPERTY_PREFIX + "allow_alter_rf_during_range_movement");
    private static final boolean allow_unsafe_transient_changes = Boolean.getBoolean(Config.PROPERTY_PREFIX + "allow_unsafe_transient_changes");

    private final String name;
    private final KeyspaceAttributes attrs;

    public AlterKeyspaceStatement(String name, KeyspaceAttributes attrs)
    {
        super();
        this.name = name;
        this.attrs = attrs;
    }

    @Override
    public String keyspace()
    {
        return name;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(name, Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(name);
        if (ksm == null)
            throw new InvalidRequestException("Unknown keyspace " + name);
        if (SchemaConstants.isLocalSystemKeyspace(ksm.name))
            throw new InvalidRequestException("Cannot alter system keyspace");
        if (ksm.isVirtual())
            throw new InvalidRequestException("Cannot alter virtual keyspaces");

        attrs.validate();

        if (attrs.getReplicationStrategyClass() == null && !attrs.getReplicationOptions().isEmpty())
            throw new ConfigurationException("Missing replication strategy class");

        if (attrs.getReplicationStrategyClass() != null)
        {
            // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
            // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
            // so doing proper validation here.
            KeyspaceParams params = attrs.asAlteredKeyspaceParams(ksm.params);
            params.validate(name);
            if (params.replication.klass.equals(LocalStrategy.class))
                throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
            validateRFChanges(ksm, params);
        }
    }

    private void validateRFChanges(KeyspaceMetadata ksm, KeyspaceParams params)
    {
        AbstractReplicationStrategy oldStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name,
                                                                                                        ksm.params.replication.klass,
                                                                                                        StorageService.instance.getTokenMetadata(),
                                                                                                        DatabaseDescriptor.getEndpointSnitch(),
                                                                                                        ksm.params.replication.options);
        AbstractReplicationStrategy newStrategy = AbstractReplicationStrategy.createReplicationStrategy(keyspace(),
                                                                                                        params.replication.klass,
                                                                                                        StorageService.instance.getTokenMetadata(),
                                                                                                        DatabaseDescriptor.getEndpointSnitch(),
                                                                                                        params.replication.options);

        validateTransientReplication(oldStrategy, newStrategy);
        warnIfIncreasingRF(oldStrategy, newStrategy);
        validateNoRangeMovements();
    }

    private void validateNoRangeMovements()
    {
        if (allow_alter_rf_during_range_movement || Gossiper.instance == null)
            return;

        Stream<InetAddressAndPort> endpoints = Stream.concat(Gossiper.instance.getLiveMembers().stream(), Gossiper.instance.getUnreachableMembers().stream());
        List<InetAddressAndPort> notNormalEndpoints = endpoints.filter(endpoint -> !Gossiper.instance.getEndpointStateForEndpoint(endpoint).isNormalState())
                                                               .collect(Collectors.toList());
        if (!notNormalEndpoints.isEmpty())
        {
            throw new ConfigurationException("Cannot alter RF while some endpoints are not in normal state (no range movements): " + notNormalEndpoints);
        }
    }

    private void validateTransientReplication(AbstractReplicationStrategy oldStrategy, AbstractReplicationStrategy newStrategy)
    {
        //If there is no read traffic there are some extra alterations you can safely make, but this is so atypical
        //that a good default is to not allow unsafe changes
        if (allow_unsafe_transient_changes)
            return;

        ReplicationFactor oldRF = oldStrategy.getReplicationFactor();
        ReplicationFactor newRF = newStrategy.getReplicationFactor();
        int oldTrans = oldRF.trans;
        int oldFull = oldRF.full;
        int newTrans = newRF.trans;
        int newFull = newRF.full;

        if (newTrans > 0)
        {
            Keyspace ks = Keyspace.open(keyspace());
            for (ColumnFamilyStore cfs: ks.getColumnFamilyStores())
            {
                if (cfs.viewManager.hasViews())
                {
                    throw new ConfigurationException("Cannot use transient replication on keyspaces using materialized views");
                }

                if (cfs.indexManager.hasIndexes())
                {
                    throw new ConfigurationException("Cannot use transient replication on keyspaces using secondary indexes");
                }
            }
        }

        //This is true right now because the transition from transient -> full lacks the pending state
        //necessary for correctness. What would happen if we allowed this is that we would attempt
        //to read from a transient replica as if it were a full replica.
        if (oldFull > newFull && oldTrans > 0)
            throw new ConfigurationException("Can't add full replicas if there are any transient replicas. You must first remove all transient replicas, then change the # of full replicas, then add back the transient replicas");

        //Don't increase transient replication factor by more than one at a time if changing number of replicas
        //Just like with changing full replicas it's not safe to do this as you could read from too many replicas
        //that don't have the necessary data. W/O transient replication this alteration was allowed and it's not clear
        //if it should be.
        //This is structured so you can convert as mnay full replicas to transient replicas as you want.
        boolean numReplicasChanged = oldTrans + oldFull != newTrans + newFull;
        if (numReplicasChanged && (newTrans > oldTrans && newTrans != oldTrans + 1))
            throw new ConfigurationException("Can only safely increase number of transients one at a time with incremental repair run in between each time");
    }

    private void warnIfIncreasingRF(AbstractReplicationStrategy oldStrategy, AbstractReplicationStrategy newStrategy)
    {
        if (newStrategy.getReplicationFactor().full > oldStrategy.getReplicationFactor().full)
            ClientWarn.instance.warn("When increasing replication factor you need to run a full (-full) repair to distribute the data.");
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(name);
        // In the (very) unlikely case the keyspace was dropped since validate()
        if (oldKsm == null)
            throw new InvalidRequestException("Unknown keyspace " + name);

        KeyspaceMetadata newKsm = oldKsm.withSwapped(attrs.asAlteredKeyspaceParams(oldKsm.params));
        MigrationManager.announceKeyspaceUpdate(newKsm, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, keyspace());
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.ALTER_KEYSPACE, keyspace(), null);
    }
}
