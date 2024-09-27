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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.SavedCommand;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.service.paxos.uncommitted.PaxosRows;

import static accord.local.Cleanup.TRUNCATE_WITH_OUTCOME;
import static accord.local.Cleanup.shouldCleanup;
import static accord.local.Status.Invalidated;
import static accord.local.Status.Truncated;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosStatePurging;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandRows.invalidated;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandRows.maybeDropTruncatedCommandColumns;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandRows.truncatedApply;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeysAccessor;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.last_executed_micros;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.last_executed_timestamp;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.last_write_timestamp;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyRows.truncateTimestampsForKeyRow;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeDurabilityOrNull;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeRouteOrNull;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeSaveStatusOrNull;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeTimestampOrNull;

public abstract class AbstractPurger extends Transformation<UnfilteredRowIterator>
{
    int compactedUnfiltered;
    final OperationType type;
    final AbstractCompactionController controller;
    final long nowInSec;
    final Runnable progressListener;

    public AbstractPurger(OperationType type, AbstractCompactionController controller, long nowInSec, Runnable progressListener)
    {
        this.type = type;
        this.controller = controller;
        this.nowInSec = nowInSec;
        this.progressListener = progressListener;
    }

    protected void onEmptyPartitionPostPurge(DecoratedKey key)
    {
        if (type == OperationType.COMPACTION)
            controller.cfs.invalidateCachedPartition(key);
    }

    protected void updateProgress()
    {
        if ((++compactedUnfiltered) % CompactionIterator.UNFILTERED_TO_UPDATE_PROGRESS == 0)
            progressListener.run();
    }

    @Override
    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        beginPartition(partition);
        UnfilteredRowIterator purged = Transformation.apply(partition, this);
        if (purged.isEmpty())
        {
            onEmptyPartitionPostPurge(purged.partitionKey());
            purged.close();
            return null;
        }

        return purged;
    }

    protected abstract void beginPartition(UnfilteredRowIterator partition);

    static class PaxosPurger extends AbstractPurger
    {
        private final long paxosPurgeGraceMicros = DatabaseDescriptor.getPaxosPurgeGrace(MICROSECONDS);
        private final Map<TableId, PaxosRepairHistory.Searcher> tableIdToHistory = new HashMap<>();

        private Token token;

        public PaxosPurger(OperationType type, AbstractCompactionController controller, long nowInSec, Runnable progressListener)
        {
            super(type, controller, nowInSec, progressListener);
        }

        @Override
        protected void beginPartition(UnfilteredRowIterator partition)
        {
            this.token = partition.partitionKey().getToken();
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            TableId tableId = PaxosRows.getTableId(row);

            switch (paxosStatePurging())
            {
                default: throw new AssertionError();
                case legacy:
                case gc_grace:
                {
                    TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
                    return row.purgeDataOlderThan(TimeUnit.SECONDS.toMicros(nowInSec - (metadata == null ? (3 * 3600) : metadata.params.gcGraceSeconds)), false);
                }
                case repaired:
                {
                    PaxosRepairHistory.Searcher history = tableIdToHistory.computeIfAbsent(tableId, find -> {
                        TableMetadata metadata = Schema.instance.getTableMetadata(find);
                        if (metadata == null)
                            return null;
                        return Keyspace.openAndGetStore(metadata).getPaxosRepairHistory().searcher();
                    });

                    return history == null ? row :
                           row.purgeDataOlderThan(history.ballotForToken(token).unixMicros() - paxosPurgeGraceMicros, false);
                }
            }
        }
    }

    public static class AccordJournalPurger extends AbstractPurger
    {
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        final Int2ObjectHashMap<CommandStores.RangesForEpoch> ranges;
        final DurableBefore durableBefore;
        final ColumnMetadata recordColumn;
        final ColumnMetadata versionColumn;
        final KeySupport<JournalKey> keySupport = JournalKey.SUPPORT;

        JournalKey key = null;
        Object builder = null;
        AccordJournalValueSerializers.FlyweightSerializer<Object, Object> serializer = null;
        Object[] lastClustering = null;
        final int userVersion;

        public AccordJournalPurger(OperationType type,
                                   AbstractCompactionController controller,
                                   long nowInSec,
                                   Runnable progressListener,
                                   Supplier<IAccordService> accordService)
        {
            super(type, controller, nowInSec, progressListener);
            IAccordService service = accordService.get();
            // TODO: test serialization version logic
            userVersion = service.journalConfiguration().userVersion();
            IAccordService.CompactionInfo compactionInfo = service.getCompactionInfo();
            this.redundantBefores = compactionInfo.redundantBefores;
            this.ranges = compactionInfo.ranges;
            this.durableBefore = compactionInfo.durableBefore;
            ColumnFamilyStore cfs = Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL);
            this.recordColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("record", false));
            this.versionColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("user_version", false));
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void beginPartition(UnfilteredRowIterator partition)
        {
            key = keySupport.deserialize(partition.partitionKey().getKey(), 0, userVersion);
            serializer = (AccordJournalValueSerializers.FlyweightSerializer<Object, Object>) key.type.serializer;
            builder = serializer.mergerFor(key);
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            beginPartition(partition);

            if (partition.isEmpty())
                return null;

            try
            {
                PartitionUpdate.SimpleBuilder newVersion = PartitionUpdate.simpleBuilder(AccordKeyspace.Journal, partition.partitionKey());

                while (partition.hasNext())
                    applyToRow((Row) partition.next());

                if (key.type != JournalKey.Type.COMMAND_DIFF)
                {
                    try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
                    {
                        serializer.reserialize(key, builder, out, userVersion);
                        newVersion.row(lastClustering)
                                  .add("record", out.asNewBuffer())
                                  .add("user_version", userVersion);
                    }

                    return newVersion.build().unfilteredIterator();
                }

                SavedCommand.Builder commandBuilder = (SavedCommand.Builder) builder;
                if (Cleanup.isSafeToCleanup(durableBefore, commandBuilder.txnId(), ranges.get(key.commandStoreId).allAt(key.timestamp.epoch())))
                    return null;

                RedundantBefore redundantBefore = redundantBefores.get(key.commandStoreId);

                if (commandBuilder.saveStatus().status == Truncated || commandBuilder.saveStatus().status == Invalidated)
                    return newVersion.build().unfilteredIterator(); // Already truncated

                Cleanup cleanup = shouldCleanup(commandBuilder.txnId(), commandBuilder.saveStatus().status,
                                                commandBuilder.durability(), commandBuilder.executeAt(), commandBuilder.route(),
                                                redundantBefore, durableBefore,
                                                false);
                switch (cleanup)
                {
                    case NO:
                        return newVersion.build().unfilteredIterator();
                    case INVALIDATE:
                    case TRUNCATE_WITH_OUTCOME:
                    case TRUNCATE:
                    case ERASE:
                        Command command = commandBuilder.construct();
                        Command newCommand = Commands.purge(command, null, cleanup);
                        newVersion = PartitionUpdate.simpleBuilder(AccordKeyspace.Journal, partition.partitionKey());

                        newVersion.row(lastClustering)
                                  .add(recordColumn.name.toString(),
                                       SavedCommand.asSerializedDiff(newCommand, userVersion));
                        return newVersion.build().unfilteredIterator();
                    default:
                        throw new IllegalStateException("Unknown cleanup: " + cleanup);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();
            ByteBuffer record = row.getCell(recordColumn).buffer();
            try (DataInputBuffer in = new DataInputBuffer(record, false))
            {
                int userVersion = Int32Type.instance.compose(row.getCell(versionColumn).buffer());
                serializer.deserialize(key, builder, in, userVersion);
                lastClustering = row.clustering().getBufferArray();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }

    static class AccordCommandsPurger extends AbstractPurger
    {
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        final Int2ObjectHashMap<CommandStores.RangesForEpoch> ranges;
        final DurableBefore durableBefore;

        int storeId;
        TxnId txnId;

        public AccordCommandsPurger(OperationType type, AbstractCompactionController controller, long nowInSec, Runnable progressListener, Supplier<IAccordService> accordService)
        {
            super(type, controller, nowInSec, progressListener);
            IAccordService.CompactionInfo compactionInfo = accordService.get().getCompactionInfo();
            this.redundantBefores = compactionInfo.redundantBefores;
            this.ranges = compactionInfo.ranges;
            this.durableBefore = compactionInfo.durableBefore;
        }

        @Override
        protected void beginPartition(UnfilteredRowIterator partition)
        {
            ByteBuffer[] partitionKeyComponents = AccordKeyspace.CommandRows.splitPartitionKey(partition.partitionKey());
            storeId = AccordKeyspace.CommandRows.getStoreId(partitionKeyComponents);
            txnId = AccordKeyspace.CommandRows.getTxnId(partitionKeyComponents);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            RedundantBefore redundantBefore = redundantBefores.get(storeId);
            // TODO (expected): if the store has been retired, this should return null
            if (redundantBefore == null)
                return row;

            // When commands end up being sliced by compaction we need this to discard tombstones and slices
            // without enough information to run the rest of the cleanup logic
            if (Cleanup.isSafeToCleanup(durableBefore, txnId, ranges.get(storeId).allAt(txnId.epoch())))
                return null;

            Cell durabilityCell = row.getCell(AccordKeyspace.CommandsColumns.durability);
            Status.Durability durability = deserializeDurabilityOrNull(durabilityCell);
            Cell executeAtCell = row.getCell(AccordKeyspace.CommandsColumns.execute_at);
            Timestamp executeAt = deserializeTimestampOrNull(executeAtCell);
            Cell routeCell = row.getCell(AccordKeyspace.CommandsColumns.route);
            Route<?> route = deserializeRouteOrNull(routeCell);
            Cell statusCell = row.getCell(AccordKeyspace.CommandsColumns.status);
            SaveStatus saveStatus = deserializeSaveStatusOrNull(statusCell);

            // With a sliced row we might not have enough columns to determine what to do so output the
            // the row unmodified and we will try again later once it merges with the rest of the command state
            // or is dropped by `durableBefore.min(txnId) == Universal`
            if (executeAt == null || durability == null || saveStatus == null || route == null)
                return row;

            Cleanup cleanup = shouldCleanup(txnId, saveStatus.status,
                                            durability, executeAt, route,
                                            redundantBefore, durableBefore,
                                            false);
            switch (cleanup)
            {
                default: throw new AssertionError(String.format("Unexpected cleanup task: %s", cleanup));
                case ERASE:
                    // Emit a tombstone so if this is slicing the command and making it not possible to determine if it
                    // can be truncated later it can still be dropped via the tombstone.
                    // Eventually the tombstone can be dropped by `durableBefore.min(txnId) == Universal`
                    // We can still encounter sliced command state just because compaction inputs are random
                    return BTreeRow.emptyDeletedRow(row.clustering(), new Row.Deletion(DeletionTime.build(row.primaryKeyLivenessInfo().timestamp(), nowInSec), false));

                case INVALIDATE:
                    return invalidated(cleanup.appliesIfNot, row, nowInSec);

                case TRUNCATE_WITH_OUTCOME:
                case TRUNCATE:
                    if (saveStatus.compareTo(cleanup.appliesIfNot) >= 0)
                        return maybeDropTruncatedCommandColumns(row, durabilityCell, executeAtCell, routeCell, statusCell);
                    return truncatedApply(cleanup.appliesIfNot,
                                          row, nowInSec, durability, durabilityCell, executeAtCell, routeCell, cleanup == TRUNCATE_WITH_OUTCOME);

                case NO:
                    return row;
            }
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }

    static class AccordTimestampsForKeyPurger extends AbstractPurger
    {
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        int storeId;
        PartitionKey partitionKey;

        public AccordTimestampsForKeyPurger(OperationType type, AbstractCompactionController controller, long nowInSec, Runnable progressListener, Supplier<IAccordService> accordService)
        {
            super(type, controller, nowInSec, progressListener);
            this.redundantBefores = accordService.get().getCompactionInfo().redundantBefores;
        }

        @Override
        protected void beginPartition(UnfilteredRowIterator partition)
        {
            ByteBuffer[] partitionKeyComponents = AccordKeyspace.TimestampsForKeyRows.splitPartitionKey(partition.partitionKey());
            storeId = AccordKeyspace.TimestampsForKeyRows.getStoreId(partitionKeyComponents);
            partitionKey = AccordKeyspace.TimestampsForKeyRows.getKey(partitionKeyComponents);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            RedundantBefore redundantBefore = redundantBefores.get(storeId);
            // TODO (expected): if the store has been retired, this should return null
            if (redundantBefore == null)
                return row;

            RedundantBefore.Entry redundantBeforeEntry = redundantBefore.get(partitionKey.toUnseekable());
            if (redundantBeforeEntry == null)
                return row;

            TxnId redundantBeforeTxnId = redundantBeforeEntry.shardRedundantBefore();

            Cell lastExecuteMicrosCell = row.getCell(last_executed_micros);
            Long last_execute_micros = null;
            if (lastExecuteMicrosCell != null && !lastExecuteMicrosCell.accessor().isEmpty(lastExecuteMicrosCell.value()))
                last_execute_micros = lastExecuteMicrosCell.accessor().getLong(lastExecuteMicrosCell.value(), 0);
            if (last_execute_micros != null && last_execute_micros < redundantBeforeTxnId.hlc())
            {
                lastExecuteMicrosCell = null;
            }

            Cell lastExecuteCell = row.getCell(last_executed_timestamp);
            Timestamp last_execute = deserializeTimestampOrNull(lastExecuteCell);
            if (last_execute != null && last_execute.compareTo(redundantBeforeTxnId) < 0)
            {
                lastExecuteCell = null;
            }

            Cell lastWriteCell = row.getCell(last_write_timestamp);
            Timestamp last_write = deserializeTimestampOrNull(lastWriteCell);
            if (last_write != null && last_write.compareTo(redundantBeforeTxnId) < 0)
            {
                lastWriteCell = null;
            }

            // No need to emit a tombstone as earlier versions of the row will also be nulled out
            // when compacted later or loaded into a commands for key
            if (lastExecuteMicrosCell == null &&
                lastExecuteCell == null &&
                lastWriteCell == null)
                return null;

            return truncateTimestampsForKeyRow(nowInSec, row, lastExecuteMicrosCell, lastExecuteCell, lastWriteCell);
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }

    static class AccordCommandsForKeyPurger extends AbstractPurger
    {
        final AccordKeyspace.CommandsForKeyAccessor accessor;
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        int storeId;
        PartitionKey partitionKey;

        public AccordCommandsForKeyPurger(OperationType type, AbstractCompactionController controller, long nowInSec, Runnable progressListener, AccordKeyspace.CommandsForKeyAccessor accessor, Supplier<IAccordService> accordService)
        {
            super(type, controller, nowInSec, progressListener);
            this.accessor = accessor;
            this.redundantBefores = accordService.get().getCompactionInfo().redundantBefores;
        }

        @Override
        protected void beginPartition(UnfilteredRowIterator partition)
        {
            ByteBuffer[] partitionKeyComponents = accessor.splitPartitionKey(partition.partitionKey());
            storeId = accessor.getStoreId(partitionKeyComponents);
            partitionKey = accessor.getKey(partitionKeyComponents);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            RedundantBefore redundantBefore = redundantBefores.get(storeId);
            // TODO (expected): if the store has been retired, this should return null
            if (redundantBefore == null)
                return row;

            RedundantBefore.Entry redundantBeforeEntry = redundantBefore.get(partitionKey.toUnseekable());
            if (redundantBeforeEntry == null)
                return row;

            return CommandsForKeysAccessor.withoutRedundantCommands(partitionKey, row, redundantBeforeEntry);
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }
}
