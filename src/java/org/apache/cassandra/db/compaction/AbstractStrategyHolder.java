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

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;

/**
 * Wrapper that's aware of how sstables are divided between separate strategies,
 * and provides a standard interface to them
 *
 * not threadsafe, calls must be synchronized by caller
 */
public abstract class AbstractStrategyHolder
{
    public static class TaskSupplier
    {
        public final int numRemaining;
        public final Supplier<AbstractCompactionTask> supplier;

        public TaskSupplier(int numRemaining, Supplier<AbstractCompactionTask> supplier)
        {
            this.numRemaining = numRemaining;
            this.supplier = supplier;
        }

        public AbstractCompactionTask getTask()
        {
            return supplier.get();
        }
    }

    public static interface DestinationRouter
    {
        int getIndexForSSTable(SSTableReader sstable);
        int getIndexForDescriptor(Descriptor descriptor);
    }

    /**
     * Maps sstables to their token partition bucket
     */
    static class GroupedSSTableContainer
    {
        private final AbstractStrategyHolder holder;
        private final Set<SSTableReader>[] groups;

        private GroupedSSTableContainer(AbstractStrategyHolder holder)
        {
            this.holder = holder;
            Preconditions.checkArgument(holder.numTokenPartitions > 0, "numTokenPartitions not set");
            groups = new Set[holder.numTokenPartitions];
        }

        void add(SSTableReader sstable)
        {
            Preconditions.checkArgument(holder.managesSSTable(sstable), "this strategy holder doesn't manage %s", sstable);
            int idx = holder.router.getIndexForSSTable(sstable);
            Preconditions.checkState(idx >= 0 && idx < holder.numTokenPartitions, "Invalid sstable index (%s) for %s", idx, sstable);
            if (groups[idx] == null)
                groups[idx] = new HashSet<>();
            groups[idx].add(sstable);
        }

        int numGroups()
        {
            return groups.length;
        }

        Set<SSTableReader> getGroup(int i)
        {
            Preconditions.checkArgument(i >= 0 && i < groups.length);
            Set<SSTableReader> group = groups[i];
            return group != null ? group : Collections.emptySet();
        }

        boolean isGroupEmpty(int i)
        {
            Preconditions.checkArgument(i >= 0 && i < groups.length);
            Set<SSTableReader> group = groups[i];
            return group == null || group.isEmpty();
        }

        boolean isEmpty()
        {
            for (int i=0; i<groups.length; i++)
            {
                Set<SSTableReader> group = groups[i];
                if (group != null && !group.isEmpty())
                    return false;

            }
            return true;
        }
    }

    protected final ColumnFamilyStore cfs;
    protected final DestinationRouter router;
    protected int numTokenPartitions = -1;

    public AbstractStrategyHolder(ColumnFamilyStore cfs, DestinationRouter router)
    {
        this.cfs = cfs;
        this.router = router;
    }

    public abstract void startup();

    public abstract void shutdown();

    public final void setStrategy(CompactionParams params, int numTokenPartitions)
    {
        Preconditions.checkArgument(numTokenPartitions > 0, "at least one token partition required");
        shutdown();
        this.numTokenPartitions = numTokenPartitions;
        setStrategyInternal(params, numTokenPartitions);
    }

    protected abstract void setStrategyInternal(CompactionParams params, int numTokenPartitions);

    public abstract boolean managesRepairedState(long repairedAt, UUID pendingRepair);

    public boolean managesSSTable(SSTableReader sstable)
    {
        return managesRepairedState(sstable.getRepairedAt(), sstable.getPendingRepair());
    }

    public abstract AbstractCompactionStrategy getStrategyFor(SSTableReader sstable);

    public abstract Iterable<AbstractCompactionStrategy> allStrategies();

    public abstract Collection<TaskSupplier> getBackgroundTaskSuppliers(int gcBefore);

    public abstract Collection<AbstractCompactionTask> getMaximalTasks(int gcBefore, boolean splitOutput);

    public abstract Collection<AbstractCompactionTask> getUserDefinedTasks(GroupedSSTableContainer sstables, int gcBefore);

    public GroupedSSTableContainer createGroupedSSTableContainer()
    {
        return new GroupedSSTableContainer(this);
    }

    public abstract void addSSTables(GroupedSSTableContainer sstables);

    public abstract void removeSSTables(GroupedSSTableContainer sstables);

    public abstract void replaceSSTables(GroupedSSTableContainer removed, GroupedSSTableContainer added);

    public abstract List<ISSTableScanner> getScanners(GroupedSSTableContainer sstables, Collection<Range<Token>> ranges);


    public abstract SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                                long keyCount,
                                                                long repairedAt,
                                                                UUID pendingRepair,
                                                                MetadataCollector collector,
                                                                SerializationHeader header,
                                                                Collection<Index> indexes,
                                                                LifecycleTransaction txn);

    public abstract int getStrategyIndex(AbstractCompactionStrategy strategy);
}
