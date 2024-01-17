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

package org.apache.cassandra.fuzz.sai;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.AgainstSutChecker;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.SelectHelper;
import org.apache.cassandra.harry.model.reconciler.PartitionState;
import org.apache.cassandra.harry.model.reconciler.Reconciler;
import org.apache.cassandra.harry.operations.FilteringQuery;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.Relation;
import org.apache.cassandra.harry.sut.DoubleWritingSut;
import org.apache.cassandra.harry.sut.QueryModifyingSut;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;

public class SingleNodeSAITest extends IntegrationTestBase
{
    private static final int RUNS = 10;
    private static final int OPERATIONS_PER_RUN = 100_000;
    private static final int MAX_PARTITION_SIZE = 10_000;
    private static final int VALIDATION_SKIP = 1000;
    private static final int NUM_PARTITIONS = 100;
    private static final int UNIQUE_CELL_VALUES = 5;

    long seed = 1;

    @Test
    public void basicSaiTest() throws InterruptedException
    {
        SchemaSpec schema = new SchemaSpec(KEYSPACE, "tbl1",
                                           Arrays.asList(ColumnSpec.ck("pk1", ColumnSpec.int64Type),
                                                         ColumnSpec.ck("pk2", ColumnSpec.asciiType(4, 100)),
                                                         ColumnSpec.ck("pk3", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType(4, 100)),
                                                         ColumnSpec.ck("ck2", ColumnSpec.asciiType, true),
                                                         ColumnSpec.ck("ck3", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType(40, 100)),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("v3", ColumnSpec.int64Type)),
                                           List.of(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType(40, 100))));

        SchemaSpec doubleWriteSchema = schema.cloneWithName(schema.keyspace, schema.table + "_debug");
        sut.schemaChange(schema.compile().cql());
        sut.schemaChange(doubleWriteSchema.compile().cql());
        sut.schemaChange(String.format("CREATE INDEX %s_sai_idx ON %s.%s (%s) USING 'sai' " +
                                       "WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};",
                                       schema.regularColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(0).name));
        sut.schemaChange(String.format("CREATE INDEX %s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.regularColumns.get(1).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(1).name));
        sut.schemaChange(String.format("CREATE INDEX %s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.regularColumns.get(2).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(2).name));
        sut.schemaChange(String.format("CREATE INDEX %s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.staticColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(0).name));

        DataTracker tracker = new DefaultDataTracker();
        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(cluster.size());
        ReplayingHistoryBuilder history = new ReplayingHistoryBuilder(seed,
                                                                      MAX_PARTITION_SIZE,
                                                                      MAX_PARTITION_SIZE,
                                                                      tracker,
                                                                      new DoubleWritingSut(sut,
                                                                                           new QueryModifyingSut(sut,
                                                                                                                 (query) -> query.replace(schema.table, doubleWriteSchema.table))),
                                                                      schema,
                                                                      rf,
                                                                      SystemUnderTest.ConsistencyLevel.QUORUM);

        for (int run = 0; run < RUNS; run++)
        {
            logger.info("Starting run {}/{}...", run + 1, RUNS);
            EntropySource random = new JdkRandomEntropySource(run);

            // Populate the array of possible values for all operations in the run:
            long[] values = new long[UNIQUE_CELL_VALUES];
            for (int i = 0; i < values.length; i++)
                values[i] = random.next();

            Thread flusher = null;

            for (int i = 0; i < OPERATIONS_PER_RUN; i++)
            {
                int partitionIndex = random.nextInt(0, NUM_PARTITIONS);

                history.visitPartition(partitionIndex)
                       .insert(random.nextInt(MAX_PARTITION_SIZE),
                               new long[] { random.nextBoolean() ? DataGenerators.UNSET_DESCR : values[random.nextInt(values.length)],
                                            random.nextBoolean() ? DataGenerators.UNSET_DESCR : values[random.nextInt(values.length)],
                                            random.nextBoolean() ? DataGenerators.UNSET_DESCR : values[random.nextInt(values.length)] },
                               new long[] { random.nextBoolean() ? DataGenerators.UNSET_DESCR : values[random.nextInt(values.length)] });

                if (random.nextFloat() > 0.99f)
                {
                    int row1 = random.nextInt(MAX_PARTITION_SIZE);
                    int row2 = random.nextInt(MAX_PARTITION_SIZE);
                    history.visitPartition(partitionIndex).deleteRowRange(Math.min(row1, row2), Math.max(row1, row2),
                                                                          random.nextBoolean(), random.nextBoolean());
                }
                else if (random.nextFloat() > 0.999f)
                {
                    history.visitPartition(partitionIndex).deleteRowSlice();
                }

                if (random.nextFloat() > 0.995f)
                {
                    history.visitPartition(partitionIndex).deleteColumns();
                }

//                if (random.nextFloat() > 0.9995f)
//                {
//                    if (flusher != null)
//                        flusher.join();
//                    flusher = new Thread(() -> cluster.get(1).nodetool("flush", schema.keyspace, schema.table));
//                    flusher.start();
//                }

                if (random.nextFloat() > 0.9995f)
                    history.visitPartition(partitionIndex).deletePartition();

                if (i % VALIDATION_SKIP != 0)
                    continue;

                logger.debug("Validating partition at index {}...", partitionIndex);

                for (int j = 0; j < 100; j++)
                {
                    List<Relation> relations = new ArrayList<>();
                    List<Relation> relationsWithoutStatic = new ArrayList<>();

                    // For one text column and 2 numeric columns, we can use between 1 and 5 total relations.
                    int num = random.nextInt(1, 5);

                    List<List<Relation.RelationKind>> pick = new ArrayList<>();
                    //noinspection ArraysAsListWithZeroOrOneArgument
                    pick.add(new ArrayList<>(Arrays.asList(Relation.RelationKind.EQ))); // text column supports only EQ
                    pick.add(new ArrayList<>(Arrays.asList(Relation.RelationKind.EQ, Relation.RelationKind.GT, Relation.RelationKind.LT)));
                    pick.add(new ArrayList<>(Arrays.asList(Relation.RelationKind.EQ, Relation.RelationKind.GT, Relation.RelationKind.LT)));

                    if (random.nextFloat() > 0.75f)
                    {
                        relations.addAll(Query.clusteringSliceQuery(schema, partitionIndex,
                                                                    random.next(),
                                                                    random.next(),
                                                                    random.nextBoolean(),
                                                                    random.nextBoolean(),
                                                                    false).relations);
                    }

                    for (int k = 0; k < num; k++)
                    {
                        int column = random.nextInt(schema.regularColumns.size());
                        Relation.RelationKind relationKind = pickKind(random, pick, column);

                        if (relationKind != null)
                            relations.add(Relation.relation(relationKind,
                                                            schema.regularColumns.get(column),
                                                            values[random.nextInt(values.length)]));
                    }

//                    if (random.nextFloat() > 0.75f)
                    {
                        relationsWithoutStatic.addAll(relations);
                        relations.add(Relation.relation(Relation.RelationKind.EQ,
                                                        schema.staticColumns.get(0),
                                                        values[random.nextInt(values.length)]));
                    }

                    long pd = history.presetSelector.pdAtPosition(partitionIndex);
                    FilteringQuery query = new FilteringQuery(pd, false, relations, schema);
                    Reconciler reconciler = new Reconciler(history.presetSelector, schema, history::visitor);
                    Set<ColumnSpec<?>> columns = new HashSet<>(schema.allColumns);

                    PartitionState modelState = reconciler.inflatePartitionState(pd, tracker, query).filter(query);

                    if (modelState.rows().size() > 0) {
                        logger.debug("Model contains {} matching rows for query {}.", modelState.rows().size(), query);
                    }


                    try
                    {
                        QuiescentChecker.validate(schema,
                                tracker,
                                columns,
                                modelState,
                                SelectHelper.execute(sut, history.clock(), query),
                                query);

                    }
                    catch (Throwable t)
                    {
                        logger.debug("Model contains {} matching rows for query without statics {}.", modelState.rows().size(), new FilteringQuery(pd, false, relationsWithoutStatic, schema));
                        QuiescentChecker.validate(schema,
                                tracker,
                                columns,
                                modelState,
                                SelectHelper.execute(sut, history.clock(),
                                        new FilteringQuery(pd, false, relationsWithoutStatic, schema)),
                                query);
                        System.out.println("SUCEEDS without a query on STATIC");

                        cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
                        QuiescentChecker.validate(schema,
                                tracker,
                                columns,
                                modelState,
                                SelectHelper.execute(sut, history.clock(), query),
                                query);
                        System.out.println("SUCEEDS after flush!");
                        throw t;

//                        Model model = new AgainstSutChecker(tracker, history.clock(), sut, schema, doubleWriteSchema);
//                        model.validate(query);

                    }


                }
            }

            if (flusher != null)
                flusher.join();
        }
    }

    private static Relation.RelationKind pickKind(EntropySource random, List<List<Relation.RelationKind>> options, int column)
    {
        Relation.RelationKind kind = null;

        if (!options.get(column).isEmpty())
        {
            List<Relation.RelationKind> possible = options.get(column);
            int chosen = random.nextInt(possible.size());
            kind = possible.remove(chosen);

            if (kind == Relation.RelationKind.EQ)
                possible.clear(); // EQ precludes LT and GT
            else
                possible.remove(Relation.RelationKind.EQ); // LT GT preclude EQ
        }

        return kind;
    }
}