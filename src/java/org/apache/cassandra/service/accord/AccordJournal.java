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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.Status.Invalidated;
import static accord.local.Status.Truncated;

public class AccordJournal implements IJournal, Shutdownable
{
    static
    {
        // make noise early if we forget to update our version mappings
        Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_51, "Expected current version to be %d but given %d", MessagingService.VERSION_51, MessagingService.current_version);
    }

    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[22]);

    private final Journal<JournalKey, Object> journal;
    private final AccordJournalTable<JournalKey, Object> journalTable;

    Node node;

    enum Status { INITIALIZED, STARTING, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    @VisibleForTesting
    public AccordJournal(Params params)
    {
        File directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        this.journal = new Journal<>("AccordJournal", directory, params, JournalKey.SUPPORT,
                                     // In Accord, we are using streaming serialization, i.e. Reader/Writer interfaces instead of materializing objects
                                     new ValueSerializer<>()
                                     {
                                         public int serializedSize(JournalKey key, Object value, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }

                                         public void serialize(JournalKey key, Object value, DataOutputPlus out, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }

                                         public Object deserialize(JournalKey key, DataInputPlus in, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }
                                     },
                                     new AccordSegmentCompactor<>());
        this.journalTable = new AccordJournalTable<>(journal, JournalKey.SUPPORT, params.userVersion());
    }

    public AccordJournal start(Node node)
    {
        Invariants.checkState(status == Status.INITIALIZED);
        this.node = node;
        status = Status.STARTING;
        journal.start();
        status = Status.STARTED;
        return this;
    }

    @Override
    public boolean isTerminated()
    {
        return status == Status.TERMINATED;
    }

    @Override
    public void shutdown()
    {
        Invariants.checkState(status == Status.STARTED);
        status = Status.TERMINATING;
        journal.shutdown();
        status = Status.TERMINATED;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Collections.singletonList(journal));
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId)
    {
        try
        {
            return loadDiffs(commandStoreId, txnId).construct();
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public SavedCommand.Builder loadDiffs(int commandStoreId, TxnId txnId)
    {
        JournalKey key = new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, commandStoreId);
        SavedCommand.Builder builder = new SavedCommand.Builder();
        journalTable.readAll(key, builder::deserializeNext);
        return builder;
    }

    @Override
    public void appendCommand(int commandStoreId, List<SavedCommand.Writer<TxnId>> outcomes, List<Command> sanityCheck, Runnable onFlush)
    {
        RecordPointer pointer = null;
        for (SavedCommand.Writer<TxnId> outcome : outcomes)
        {
            JournalKey key = new JournalKey(outcome.key(), JournalKey.Type.COMMAND_DIFF, commandStoreId);
            pointer = journal.asyncWrite(key, outcome, SENTINEL_HOSTS);
        }

        // If we need to perform sanity check, we can only rely on blocking flushes. Otherwise, we may see into the future.
        if (sanityCheck != null)
        {
            Condition condition = Condition.newOneTimeCondition();
            journal.onFlush(pointer, condition::signal);
            condition.awaitUninterruptibly();

            for (Command check : sanityCheck)
                sanityCheck(commandStoreId, check);

            onFlush.run();
        }
        else
        {
            journal.onFlush(pointer, onFlush);
        }
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTesting()
    {
        journal.closeCurrentSegmentForTesting();
    }

    public void sanityCheck(int commandStoreId, Command orig)
    {
        try
        {
            SavedCommand.Builder diffs = loadDiffs(commandStoreId, orig.txnId());
            diffs.forceResult(orig.result());
            // We can only use strict equality if we supply result.
            Command reconstructed = diffs.construct();
            Invariants.checkState(orig.equals(reconstructed),
                                  '\n' +
                                  "Original:      %s\n" +
                                  "Reconstructed: %s\n" +
                                  "Diffs:         %s", orig, reconstructed, diffs);
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        journal.truncateForTesting();
    }

    @VisibleForTesting
    public void replay()
    {
        // TODO: optimize replay memory footprint
        class ToApply
        {
            final JournalKey key;
            final Command command;

            ToApply(JournalKey key, Command command)
            {
                this.key = key;
                this.command = command;
            }
        }

        List<ToApply> toApply = new ArrayList<>();
        try (AccordJournalTable.KeyOrderIterator<JournalKey> iter = journalTable.readAll())
        {
            JournalKey key = null;
            final SavedCommand.Builder builder = new SavedCommand.Builder();
            while ((key = iter.key()) != null)
            {
                JournalKey finalKey = key;
                iter.readAllForKey(key, (segment, position, local, buffer, hosts, userVersion) -> {
                    Invariants.checkState(finalKey.equals(local));
                    try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                    {
                        builder.deserializeNext(in, userVersion);
                    }
                    catch (IOException e)
                    {
                        // can only throw if serializer is buggy
                        throw new RuntimeException(e);
                    }
                });

                Command command = builder.construct();
                AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().forId(key.commandStoreId);
                commandStore.loader().load(command).get();
                if (command.saveStatus().compareTo(SaveStatus.Applying) >= 0 && !command.is(Invalidated) && !command.is(Truncated))
                    toApply.add(new ToApply(key, command));
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not replay journal.", t);
        }
        toApply.sort(Comparator.comparing(v -> v.command.executeAt()));
        for (ToApply apply : toApply)
        {
            AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().forId(apply.key.commandStoreId);
            commandStore.loader().apply(apply.command);
        }
    }

    private interface FlyweightSerializer<KEY, BUILDER>
    {
        void serialize(KEY key, BUILDER from, DataOutputPlus out, int userVersion) throws IOException;
        void deserialize(KEY key, BUILDER into, DataInputPlus in, int userVersion) throws IOException;
    }
}