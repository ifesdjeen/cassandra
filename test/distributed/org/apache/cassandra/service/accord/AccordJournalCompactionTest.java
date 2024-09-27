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
import java.nio.file.Files;
import java.util.NavigableMap;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.ReducingRangeMap;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.CommandStores.RangesForEpoch;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.BootstrapBeganAtAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.IdentityAccumulator;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.RedundantBeforeAccumulator;


public class AccordJournalCompactionTest
{
    @BeforeClass
    public static void setUp() throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        StorageService.instance.registerMBeans();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());
        StorageService.instance.initServer();
        Keyspace.setInitialized();
    }

    @Test
    public void segmentMergeTest() throws IOException, InterruptedException
    {
        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();

        Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());
        Gen<DurableBefore> durableBeforeGen = AccordGenerators.durableBeforeGen(DatabaseDescriptor.getPartitioner());
        Gen<ReducingRangeMap<Timestamp>> rejectBeforeGen = AccordGenerators.rejectBeforeGen(DatabaseDescriptor.getPartitioner());
        Gen<Ranges> rangeGen = AccordGenerators.ranges(DatabaseDescriptor.getPartitioner());
        Gen<TxnId> txnIdGen = AccordGens.txnIds(Gens.pick(Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range);
        Gen<NavigableMap<Timestamp, Ranges>> safeToReadGen = AccordGenerators.safeToReadGen(DatabaseDescriptor.getPartitioner());
        Gen<RangesForEpoch.Snapshot> rangesForEpochGen = AccordGenerators.rangesForEpoch(DatabaseDescriptor.getPartitioner());

        AccordJournal journal = new AccordJournal(new TestParams()
        {
            @Override
            public int segmentSize()
            {
                return 1024 * 1024;
            }

            @Override
            public boolean enableCompaction()
            {
                return false;
            }
        });
        try
        {
            journal.start(null);
            Timestamp timestamp = Timestamp.NONE;

            RandomSource rs = new DefaultRandom();
            Gen<TxnId> bootstrappedAtTxn = new Gen<TxnId>()
            {
                TxnId prev = txnIdGen.next(rs);
                public TxnId next(RandomSource random)
                {
                    prev = new TxnId(prev.epoch() + 1, prev.hlc() + random.nextInt(1, 100), prev.kind(), prev.domain(), prev.node);
                    return prev;
                }
            };

            RedundantBeforeAccumulator redundantBeforeAccumulator = new RedundantBeforeAccumulator();
            DurableBeforeAccumulator durableBeforeAccumulator = new DurableBeforeAccumulator();
            IdentityAccumulator<ReducingRangeMap<Timestamp>> rejectBeforeAccumulator = new IdentityAccumulator<>(new ReducingRangeMap<>());
            BootstrapBeganAtAccumulator bootstrapBeganAtAccumulator = new BootstrapBeganAtAccumulator();
            IdentityAccumulator<NavigableMap<Timestamp, Ranges>> safeToReadAccumulator = new IdentityAccumulator<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
            IdentityAccumulator<RangesForEpoch.Snapshot> rangesForEpochAccumulator = new IdentityAccumulator<>(null);
            int count = 1_000;
            Condition condition = Condition.newOneTimeCondition();
            for (int i = 0; i <= count; i++)
            {
                timestamp = timestamp.next();
                AccordSafeCommandStore.FieldUpdates updates = new AccordSafeCommandStore.FieldUpdates();
                updates.durableBefore = durableBeforeGen.next(rs);
                updates.redundantBefore = redundantBeforeGen.next(rs);
                updates.rejectBefore = rejectBeforeGen.next(rs);
                if (i % 100 == 0)
                    updates.newBootstrapBeganAt = new AccordSafeCommandStore.Sync(bootstrappedAtTxn.next(rs), rangeGen.next(rs));
                updates.newSafeToRead = safeToReadGen.next(rs);
                updates.rangesForEpoch = rangesForEpochGen.next(rs);

                if (i == count)
                    journal.persistStoreState(1, updates, condition::signal);
                else
                    journal.persistStoreState(1, updates, null);

                redundantBeforeAccumulator.update(updates.redundantBefore);
                durableBeforeAccumulator.update(updates.durableBefore);
                rejectBeforeAccumulator.update(updates.rejectBefore);
                if (updates.newBootstrapBeganAt != null)
                    bootstrapBeganAtAccumulator.update(updates.newBootstrapBeganAt);
                safeToReadAccumulator.update(updates.newSafeToRead);
                rangesForEpochAccumulator.update(updates.rangesForEpoch);
            }

            condition.await();

            journal.closeCurrentSegmentForTesting();
            journal.runCompactorForTesting();

            Assert.assertEquals(redundantBeforeAccumulator.get(), journal.loadRedundantBefore(1));
            Assert.assertEquals(durableBeforeAccumulator.get(), journal.loadDurableBefore(1));
            Assert.assertEquals(rejectBeforeAccumulator.get(), journal.loadRejectBefore(1));
            Assert.assertEquals(bootstrapBeganAtAccumulator.get(), journal.loadBootstrapBeganAt(1));
            Assert.assertEquals(safeToReadAccumulator.get(), journal.loadSafeToRead(1));
            Assert.assertEquals(rangesForEpochAccumulator.get(), journal.loadRangesForEpoch(1));
        }
        finally
        {
            journal.shutdown();
        }
    }

    @Test
    public void mergeKeysTest()
    {
        AccordJournal accordJournal = new AccordJournal(TestParams.INSTANCE);
        try
        {
            accordJournal.start(null);
            Gen<Timestamp> timestampGen = AccordGens.timestamps();
            // TODO: we might benefit from some unification of generators
            Gen<RedundantBefore> redundantBeforeGen = AccordGenerators.redundantBefore(DatabaseDescriptor.getPartitioner());
            RandomSource rng = new DefaultRandom();
            // Probably all redundant befores will be written with the same timestamp?
            timestampGen.next(rng);
            RedundantBefore expected = RedundantBefore.EMPTY;
            for (int i = 0; i < 10; i++)
            {
                RedundantBefore redundantBefore = redundantBeforeGen.next(rng);
                expected = RedundantBefore.merge(expected, redundantBefore);
                accordJournal.appendRedundantBefore(1, redundantBefore, () -> {});
            }

            RedundantBefore actual = accordJournal.loadRedundantBefore(1);
            Assert.assertEquals(expected, actual);
        }
        finally
        {
            accordJournal.shutdown();
        }
    }

    private static TxnId nextTxnId(TxnId txnId)
    {
        return new TxnId(txnId.next(), txnId.kind(), txnId.domain());
    }
}
