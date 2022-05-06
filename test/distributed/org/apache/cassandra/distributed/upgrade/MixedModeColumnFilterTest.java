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

package org.apache.cassandra.distributed.upgrade;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.model.OpSelectors;
import harry.model.QuiescentChecker;
import harry.model.SelectHelper;
import harry.model.clock.OffsetClock;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.operations.Query;
import harry.operations.WriteHelper;
import harry.reconciler.PartitionState;
import harry.util.BitSet;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.fuzz.UpgradableInJvmSut;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ExpiringMemoizingSupplier;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class MixedModeColumnFilterTest
{
    private static Logger logger = LoggerFactory.getLogger(MixedModeColumnFilterTest.class);
    final List<String> majorVersions = Arrays.asList("3.0.27",
                                                    "4.0.4");// with cluster version dependant ColumnFilter serializer

    //     ant -Dbase.version=4.1.0 dtest-jar @ 3f5a2cf5d3068b454dbfb2cd190e264cf3e5ceb7
    final List<String> minorVersions = Arrays.asList("4.1",
                                                    "4.2");// with cluster version dependant ColumnFilter serializer

    static class InstanceVersions extends HashMap<Integer, String>
    {
        public InstanceVersions(Map<Integer,String> source)
        {
            super(source);
        }
    }

    public static class ModelState
    {
        public long lts = 0;
        public final Map<Long, PartitionState> state;

        public ModelState(Map<Long, PartitionState> state)
        {
            this.state = state;
        }
    }

    public static class DescriptorSelector
    {
        private static long PD_STREAM = System.nanoTime();
        private final OpSelectors.Rng rng;
        private final SchemaSpec schema;

        public DescriptorSelector(OpSelectors.Rng rng, SchemaSpec schema)
        {
            this.schema = schema;
            this.rng = rng;
        }

        public long pd(int pdIdx)
        {
            long pd = this.rng.randomNumber(pdIdx + 1, PD_STREAM);
            long adjusted = schema.adjustPdEntropy(pd);
            assert adjusted == pd : "Partition descriptors not utilising all entropy bits are not supported.";
            return pd;
        }

        public long cd(int pdIdx, int cdIdx)
        {
            long cd = this.rng.randomNumber(cdIdx + 1, pd(pdIdx));
            long adjusted = schema.adjustCdEntropy(cd);
            assert adjusted == cd : "Clustering descriptors not utilising all entropy bits are not supported.";
            return cd;
        }
    }

    public static Surjections.Surjection<SchemaSpec> defaultSchemaSpecGen(String ks, String table)
    {
        return new SchemaGenerators.Builder(ks, () -> table)
               .partitionKeySpec(1, 2,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.asciiType(5, 256))
               .clusteringKeySpec(1, 2,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.asciiType(2, 3),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.asciiType(2, 3)))
               .regularColumnSpec(2, 2,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.asciiType(5, 3))
               .staticColumnSpec(2, 2,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.asciiType(4, 3))
               .surjection();
    }

    public static Configuration.ConfigurationBuilder sharedConfiguration(long seed, SchemaSpec schema, SystemUnderTest sut)
    {
        return new Configuration.ConfigurationBuilder().setSeed(seed)
                                                       .setClock(() -> new OffsetClock(100000))
                                                       .setCreateSchema(true)
                                                       .setTruncateTable(false)
                                                       .setDropSchema(true)
                                                       .setSchemaProvider((seed1, sut1) -> schema)
                                                       .setClusteringDescriptorSelector(sharedCDSelectorConfiguration().build())
                                                       .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, 200))
                                                       .setSUT(() -> sut);
    }

    public static Configuration.CDSelectorConfigurationBuilder sharedCDSelectorConfiguration()
    {
        return new Configuration.CDSelectorConfigurationBuilder()
        .setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(2))
        .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(2))
        .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                 .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                 .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                 .addWeight(OpSelectors.OperationKind.DELETE_RANGE, 1)
                                 .addWeight(OpSelectors.OperationKind.DELETE_SLICE, 1)
                                 .addWeight(OpSelectors.OperationKind.DELETE_PARTITION, 1)
                                 .addWeight(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS, 5)
                                 .addWeight(OpSelectors.OperationKind.INSERT_WITH_STATICS, 20)
                                 .addWeight(OpSelectors.OperationKind.INSERT, 20)
                                 .addWeight(OpSelectors.OperationKind.UPDATE_WITH_STATICS, 25)
                                 .addWeight(OpSelectors.OperationKind.UPDATE, 25)
                                 .build());
    }

    public static <T> BitSet subsetToBitset(List<T> superset, Set<T> subset)
    {
        BitSet bitSet = BitSet.allUnset(superset.size());
        for (int i = 0; i < superset.size(); i++)
        {
            if (subset.contains(superset.get(i)))
                bitSet.set(i);
        }
        return bitSet;
    }

    public static boolean isValidSubset(List<ColumnSpec<?>> superset, BitSet bitSet)
    {
        boolean hasRegular = false;
        for (int i = 0; i < superset.size(); i++)
        {
            ColumnSpec<?> column = superset.get(i);
            if (column.kind == ColumnSpec.Kind.REGULAR && bitSet.isSet(i))
                hasRegular = true;
        }

        return hasRegular;
    }

    public static boolean hasAnyRegularColumns(Set<ColumnSpec<?>> columns)
    {
        for (ColumnSpec<?> column : columns)
        {
            if (column.kind == ColumnSpec.Kind.REGULAR)
                return true;
        }

        return false;
    }

    public static Set<ColumnSpec<?>> subset(List<ColumnSpec<?>> superset, BitSet bitSet)
    {
        Set<ColumnSpec<?>> subset = new HashSet<>();
        for (int i = 0; i < superset.size(); i++)
        {
            if (bitSet.isSet(i))
                subset.add(superset.get(i));
        }

        return subset;
    }

    static class TestHolder
    {
        final SchemaSpec schema;
        final Configuration config;
        final Run run;
        final DescriptorSelector test;
        final ModelState state;
        final Random rng;

        public TestHolder(SchemaSpec schema, Configuration config, Run run, DescriptorSelector test, ModelState state, Random rng) {
            this.schema = schema;
            this.config = config;
            this.run = run;
            this.test = test;
            this.state = state;
            this.rng = rng;
        }
    }

    void populate(UpgradableInJvmSut sut, TestHolder holder, int rowsPerPartition)
    {
        final SchemaSpec schema = holder.schema;
        final Run run = holder.run;
        DescriptorSelector test = holder.test;
        ModelState state = holder.state;
        final Random rng = holder.rng;

        int partitionIdx = 0;
        final long RUNS = 10;

        for (int i = 1; i <= RUNS; i++)
        {
            BitSet subset = BitSet.allUnset(schema.allColumns.size());
            for (int j = 0; j < subset.size(); j++)
            {
                if (rng.nextBoolean())
                    subset.set(j);
            }
            if (!isValidSubset(schema.allColumns, subset))
                continue;

            int pdIdx = partitionIdx++;
            long pd = test.pd(pdIdx);

            for (int j = 0; j < 10; j++)
            {
                int cdIdx = rng.nextInt(rowsPerPartition);
                long cd = test.cd(pdIdx, cdIdx);

                long[] vds = run.descriptorSelector.descriptors(pd, cd, state.lts, 0, schema.regularColumns,
                                                                schema.regularColumnsMask(),
                                                                subset,
                                                                schema.regularColumnsOffset);
                long[] sds = run.descriptorSelector.descriptors(pd, cd, state.lts, 0, schema.staticColumns,
                                                                schema.staticColumnsMask,
                                                                subset,
                                                                schema.staticColumnsOffset);

                CompiledStatement statement = WriteHelper.inflateUpdate(schema, pd, cd, vds, sds, run.clock.rts(state.lts));
                //logger.info("Update {} bindings {}", statement.cql(), Arrays.toString(statement.bindings()));
                sut.cluster.coordinator(1).execute(statement.cql(), ConsistencyLevel.QUORUM, statement.bindings());

                PartitionState partitionState = state.state.get(pd);
                if (partitionState == null)
                {
                    partitionState = new PartitionState(pd, schema);
                    state.state.put(pd, partitionState);
                }

                partitionState.writeStaticRow(sds, state.lts);
                partitionState.write(cd, vds, state.lts, true);

                state.lts++;
            }
        }
    }

    static boolean selectsNonPartitionColumns(Collection<ColumnSpec<?>> selection)
    {
        if (selection == null)
            return true;
        for (ColumnSpec<?> column : selection)
        {
            if (column.kind == ColumnSpec.Kind.CLUSTERING || column.kind == ColumnSpec.Kind.REGULAR)
                return true;
        }
        return false;
    }

    static Set<ColumnSpec<?>> selectColumnSpecs(List<ColumnSpec<?>> columns, int selected)
    {
        Set<ColumnSpec<?>> result = new HashSet<>();
        for (int index = 0; selected != 0; selected>>=1, index++)
        {
            if ((selected & 1) == 1)
                result.add(columns.get(index));
        }
        return result;
    }

    final static int STATIC_COLUMN_LIMIT = 2;
    final static int REGULAR_COLUMN_LIMIT = 2;

    static List<Set<ColumnSpec<?>>> significantColumnSelections(SchemaSpec schema)
    {
        List<Set<ColumnSpec<?>>> result = new ArrayList<>();
        result.add(null); // wildcard queries
        final int pkMax = 1 << schema.partitionKeys.size();
        for (int pkBits = 0; pkBits < pkMax; pkBits++)
        {
            Set<ColumnSpec<?>> pkCols = selectColumnSpecs(schema.partitionKeys, pkBits);

            final int ckMax = 1 << schema.clusteringKeys.size();
            for (int ckBits = 0; ckBits < ckMax; ckBits++)
            {
                Set<ColumnSpec<?>> keyCols = selectColumnSpecs(schema.clusteringKeys, ckBits);
                keyCols.addAll(pkCols);

                addRegularAndStaticCombinations(schema, result, keyCols);
            }
        }
        return result;
    }

    private static void addRegularAndStaticCombinations(SchemaSpec schema, List<Set<ColumnSpec<?>>> result, Set<ColumnSpec<?>> keyCols)
    {
        final int sMax = 1 << Math.min(STATIC_COLUMN_LIMIT, schema.regularColumns.size());; // none, first, second, both or all

        for (int sBits = 0; sBits < sMax; sBits++)
        {
            Set<ColumnSpec<?>> sCols = selectColumnSpecs(schema.staticColumns, sBits);

            if (schema.staticColumns.size() > STATIC_COLUMN_LIMIT)
            {
                Set<ColumnSpec<?>> complementsCols = new HashSet<>(schema.staticColumns);
                complementsCols.removeAll(sCols);
                complementsCols.addAll(keyCols);
                addRegularCombinations(schema, result, complementsCols);
            }

            sCols.addAll(keyCols);
            addRegularCombinations(schema, result, sCols);
        }
    }

    private static void addRegularCombinations(SchemaSpec schema, List<Set<ColumnSpec<?>>> result, Set<ColumnSpec<?>> otherCols)
    {
        final int rMax = 1 << Math.min(REGULAR_COLUMN_LIMIT, schema.regularColumns.size());
        for (int rBits = 0; rBits < rMax; rBits++)
        {
            Set<ColumnSpec<?>> rCols = selectColumnSpecs(schema.regularColumns, rBits);

            if (schema.regularColumns.size() > REGULAR_COLUMN_LIMIT)
            {
                Set<ColumnSpec<?>> complementrCols = new HashSet<>(schema.regularColumns);
                complementrCols.removeAll(rCols);
                complementrCols.addAll(otherCols);

                result.add(complementrCols);
            }

            rCols.addAll(otherCols);
            result.add(rCols);
        }
    }

    void validate(UpgradableInJvmSut sut, TestHolder holder, Random rng)
    {
        final SchemaSpec schema = holder.schema;
        final Run run = holder.run;
        final ModelState state = holder.state;

        List<Set<ColumnSpec<?>>> testSelections = significantColumnSelections(schema);

        // Validate that all partitions correspond to our expectations
        state.state.forEach((pd, partitionState) -> {
            List<Long> clusteringDescriptors = new ArrayList<>(partitionState.rows().keySet());
            for (Set<ColumnSpec<?>> selection : testSelections)
            {
                if (selection != null && selection.isEmpty())
                    continue;
                long cd1tmp = clusteringDescriptors.get(rng.nextInt(clusteringDescriptors.size()));
                long cd2tmp;
                while (true)
                {
                    long tmp = clusteringDescriptors.get(rng.nextInt(clusteringDescriptors.size()));
                    if (tmp != cd1tmp)
                    {
                        cd2tmp = tmp;
                        break;
                    }
                }

                long cd1 = Math.min(cd1tmp, cd2tmp);
                long cd2 = Math.max(cd1tmp, cd2tmp);

                Query query;

                // Select entire partition
                query = Query.selectPartition(schema, pd, false);
                QuiescentChecker.validate(schema,
                                          selection,
                                          state.state.get(pd),
                                          SelectHelper.execute(sut, run.clock, query, selection),
                                          query);

                if (selectsNonPartitionColumns(selection))
                {
                    // Select single row
                    query = Query.singleClustering(schema, pd, cd1, false);
                    QuiescentChecker.validate(schema,
                                              selection,
                                              state.state.get(pd).apply(query),
                                              SelectHelper.execute(sut, run.clock, query, selection),
                                              query);


                    // Select slice, ie a > ? AND b = ?
                    try
                    {
                        query = Query.clusteringSliceQuery(schema, pd, cd1, rng.nextLong(), true, true, false);
                    }
                    catch (IllegalArgumentException impossibleQuery)
                    {
                        continue;
                    }

                    QuiescentChecker.validate(schema,
                                              selection,
                                              state.state.get(pd).apply(query),
                                              SelectHelper.execute(sut, run.clock, query, selection),
                                              query);


                    // Select range, ie a > ? AND a < ?
                    try
                    {
                        query = Query.clusteringRangeQuery(schema, pd, cd1, cd2, rng.nextLong(), true, true, false);
                    }
                    catch (IllegalArgumentException impossibleQuery)
                    {
                        continue;
                    }
                    QuiescentChecker.validate(schema,
                                              selection,
                                              state.state.get(pd).apply(query),
                                              SelectHelper.execute(sut, run.clock, query, selection),
                                              query);
                }
            }
        });
    }

    @BeforeClass
    static public void beforeClass() throws Throwable
    {
        ICluster.setup();
    }

    @Test
    public void majorUpgrade() throws Throwable
    {
        run(majorVersions);
    }

    @Test
    public void minorUpgrade() throws Throwable
    {
        run(minorVersions);
    }

    public void run(final List<String> stringVersions) throws Throwable
    {
        final List<CassandraVersion> versions = stringVersions
        .stream()
        .map(CassandraVersion::new)
        .collect(Collectors.toList());

        // 4 node cluster - when in mixed mode, two of each version so they can be tested as local and remote.
        final int nodeCount = 4;
        int rowsPerPartition = 10;
        final Random rng = new Random(1);

        // Check versions required for test are present
        Versions dtestVersions = Versions.find();
        List<Versions.Version> upgradeOrder = stringVersions.stream()
                                                      .map(v -> dtestVersions.get(v))
                                                      .collect(Collectors.toList());
        Versions.Version earliestVersion = upgradeOrder.get(0);


        // Prepare Cluster
        java.util.function.Consumer<IInstanceConfig> configUpdater = config -> config.with(Feature.NETWORK, Feature.GOSSIP);
        Consumer<UpgradeableCluster.Builder > builderUpdater = builder -> builder.withInstanceInitializer(BBInstaller::installUpgradeVersionBB);
        try (UpgradeableCluster cluster = UpgradeableCluster.create(nodeCount, earliestVersion, configUpdater, builderUpdater))
        {
            cluster.schemaChange("CREATE KEYSPACE harry WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + nodeCount + "};");

            UpgradableInJvmSut sut = new UpgradableInJvmSut(cluster);

            TestHolder[] tests = new TestHolder[10];
            for (int schemaDescriptor = 0; schemaDescriptor < tests.length; schemaDescriptor++)
            {
                SchemaSpec schema = defaultSchemaSpecGen("harry", "tbl" + schemaDescriptor).inflate(schemaDescriptor);
                logger.info("Schema {} -> {}", schemaDescriptor, schema.toString());
                cluster.schemaChange(schema.compile().cql());

                Configuration config = sharedConfiguration(1, schema, sut).build();
                Run run = config.createRun();
                DescriptorSelector test = new DescriptorSelector(run.rng, schema);

                ModelState state = new ModelState(new HashMap<>());
                tests[schemaDescriptor] = new TestHolder(schema, config, run, test, state, rng);

                populate(sut, tests[schemaDescriptor], rowsPerPartition);
                validate(sut, tests[schemaDescriptor], rng);
            }

            for (int upgradeOrderIdx = 1; upgradeOrderIdx < upgradeOrder.size() - 1; upgradeOrderIdx++)
            {
                Versions.Version upgradeVersion = upgradeOrder.get(upgradeOrderIdx);
                final InstanceVersions halfUpgradedInstanceVersions = upgradeInstances(cluster, upgradeVersion, 1, 2);
                final InstanceVersions fullyUpgradedInstanceVersions = upgradeInstances(cluster, upgradeVersion, 3, 4);
            }

            // TODO - fix value of org.apache.cassandra.gms.Gossiper.upgradeFromVersionMemoized
            BBInstaller.shouldIntercept = true;
            BBInstaller.upgradeVersionOverride.put(cluster.get(1).config().broadcastAddress().getAddress(), versions.get(versions.size() - 2).toString());
            BBInstaller.upgradeVersionOverride.put(cluster.get(2).config().broadcastAddress().getAddress(), versions.get(versions.size() - 2).toString());

            // Upgrade two cluster nodes, leave two on version[-1]
            Versions.Version upgradeVersion = upgradeOrder.get(upgradeOrder.size()-1);

            final InstanceVersions halfUpgradedInstanceVersions = upgradeInstances(cluster, upgradeVersion, 1, 2);

            for (int schemaDescriptor = 0; schemaDescriptor < tests.length; schemaDescriptor++)
            {
                validate(sut, tests[schemaDescriptor], rng);
            }

            // Upgrade final two cluster nodes - simulating prepare statements
            BBInstaller.upgradeVersionOverride.put(cluster.get(3).config().broadcastAddress().getAddress(), "null");
            BBInstaller.upgradeVersionOverride.put(cluster.get(4).config().broadcastAddress().getAddress(), "null");
            final InstanceVersions fullyUpgradedInstanceVersions = upgradeInstances(cluster, upgradeVersion, 3, 4);
            for (int schemaDescriptor = 0; schemaDescriptor < tests.length; schemaDescriptor++)
            {
                validate(sut, tests[schemaDescriptor], rng);
            }

            // Update and select from fully upgraded cluster with upgradeFromVersion cleared on all hosts
            BBInstaller.upgradeVersionOverride.clear();

            for (int schemaDescriptor = 0; schemaDescriptor < tests.length; schemaDescriptor++)
            {
                validate(sut, tests[schemaDescriptor], rng);
            }
        }
    }

    private InstanceVersions upgradeInstances(UpgradeableCluster cluster, Versions.Version upgradeTo, int... instanceIdsArray)
    {
        Arrays.stream(instanceIdsArray).forEach(instanceId -> {
            logger.info("Upgrading instance{} to {}", instanceId, upgradeTo.version);
            IUpgradeableInstance instance = cluster.get(instanceId);
            try
            {
                long preShutdownLogs = instance.logs().mark();
                instance.shutdown(true).get(1, TimeUnit.MINUTES);
                instance.setVersion(upgradeTo);
                instance.startup();
                instance.logs().watchFor(preShutdownLogs, "[0-9] NORMAL$");
                logger.info("Upgraded instance{} to {} - status NORMAL", instanceId, upgradeTo.version);
            }
            catch (Throwable tr)
            {
                throw new RuntimeException("Unable to upgrade instance " + instanceId + " to version " + upgradeTo.version, tr);
            }
        });
        return new InstanceVersions(cluster.stream()
                                           .collect(Collectors.toMap(i -> i.config().num(),
                                                                     IUpgradeableInstance::getReleaseVersionString)));
    }

    @Shared
    public static class BBInstaller
    {
        static volatile boolean shouldIntercept = false;
        public static ConcurrentMap<InetAddress, String> upgradeVersionOverride = new ConcurrentHashMap<>();
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            if (!shouldIntercept)
                return;
            try
            {
                // Hopefully throw an exception if not supported

                new ByteBuddy().rebase(Gossiper.class)
                               .method(named("upgradeFromVersion"))
                               .intercept(MethodDelegation.to(BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                logger.info("... but no class def", noClassDefFoundError);
            }
            catch (Throwable tr)
            {
                logger.info("other stuff", tr);
            }
        }

    }
    public static class BBInterceptor
    {
        @SuppressWarnings("unused")
        public static ExpiringMemoizingSupplier.ReturnValue<CassandraVersion> upgradeFromVersionSupplier(@SuperCall Callable<ExpiringMemoizingSupplier.ReturnValue<CassandraVersion>> zuper)
        {
            try
            {
                String overide = BBInstaller.upgradeVersionOverride.get(FBUtilities.getJustBroadcastAddress());

                if (overide != null && !overide.equals("null"))
                    return new ExpiringMemoizingSupplier.NotMemoized(overide.equals("null") ? null : new CassandraVersion(overide));
                else
                    return new ExpiringMemoizingSupplier.NotMemoized(zuper.call().value());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
