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

import java.util.EnumSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.SaveStatus;
import accord.utils.Gen;
import accord.utils.LazyToString;
import accord.utils.ReflectionUtils;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.SavedCommand.Fields;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.AccordGenerators;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.SavedCommand.getFlags;

public class SavedCommandTest
{
    private static final EnumSet<Fields> ALL = EnumSet.allOf(Fields.class);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        TableMetadata tbl = Schema.instance.getTableMetadata("ks", "tbl");
        Assert.assertEquals(TransactionalMode.full, tbl.params.transactionalMode);
        StorageService.instance.initServer();
    }

    @Test
    public void allNull()
    {
        int flags = getFlags(null, null);
        assertMissing(flags, ALL);
    }

    @Test
    public void serde()
    {
        Gen<AccordGenerators.CommandBuilder> gen = AccordGenerators.commandsBuilder();
        DataOutputBuffer out = new DataOutputBuffer();
        qt().withSeed(-1451772586194177400L)
            .forAll(gen)
            .check(cmdBuilder -> {
                int userVersion = 1; //TODO (maintance): where can we fetch all supported versions?
                for (SaveStatus saveStatus : SaveStatus.values())
                {
                    out.clear();
                    Command orig;
                    try
                    {
                        orig = cmdBuilder.build(saveStatus);
                    }
                    catch (Throwable t)
                    {
                        // TODO (desired): improve the generator so that it does not produce invalid commands
                        // Skip unbuildable and non-serializable commands
                        return;
                    }
                    SavedCommand.serialize(null, orig, out, userVersion);
                    SavedCommand.Builder builder = new SavedCommand.Builder();
                    builder.deserializeNext(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), userVersion);
                    // We are not persisting the result, so force it for strict equality
                    builder.forceResult(orig.result());

                    Command reconstructed = builder.construct();
                    if (!reconstructed.equals(orig))
                        reconstructed.equals(orig);
                    Assertions.assertThat(reconstructed)
                              .describedAs("lhs=expected\nrhs=actual\n%s", new LazyToString(() -> ReflectionUtils.recursiveEquals(orig, reconstructed).toString()))
                              .isEqualTo(orig);
                }
            });
    }

    private void assertMissing(int flags, Set<Fields> missing)
    {
        SoftAssertions checks = new SoftAssertions();
        for (Fields field : missing)
        {
            checks.assertThat(SavedCommand.getFieldChanged(field, flags))
                  .describedAs("field %s changed", field)
                  .isFalse();
            checks.assertThat(SavedCommand.getFieldIsNull(field, flags))
                  .describedAs("field %s not null", field)
                  .isTrue();
        }
        checks.assertAll();
    }
}