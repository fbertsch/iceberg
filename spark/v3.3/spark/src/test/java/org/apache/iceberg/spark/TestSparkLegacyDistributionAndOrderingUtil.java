/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark;

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE;

public class TestSparkLegacyDistributionAndOrderingUtil extends SparkTestBaseWithCatalog {
    /***
     * These tests are to ensure that setting the property SparkSQLProperties.USE_DEFAULT_DISTRIBUTION_MODE_CHANGE to
     * false reverts Spark's distribution and ordering behaviour to how it worked pre Iceberg 1.2.x.
     *
     * OSS change ref : https://github.com/apache/iceberg/pull/6828
     * Netflix Internal feature flag ref : https://github.netflix.net/corp/bdp-iceberg/pull/628
     *
     * The tests for the OSS behaviour can be found in #TestSparkDistributionAndOrderingUtil.java
     */

    private static final Distribution UNSPECIFIED_DISTRIBUTION = Distributions.unspecified();

    @Before
    public void before() {
        spark.conf().set(SparkSQLProperties.USE_DEFAULT_DISTRIBUTION_MODE_CHANGE, "false");
    }

    @After
    public void after() {
        sql("DROP TABLE IF EXISTS %s", tableName);
        spark.conf().unset(SparkSQLProperties.USE_DEFAULT_DISTRIBUTION_MODE_CHANGE);
    }

    @Test
    public void testDefaultWritePartitionedUnsortedTable(){
        sql(
                "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
                        + "USING iceberg "
                        + "PARTITIONED BY (date, days(ts))",
                tableName);

        Table table = validationCatalog.loadTable(tableIdent);

        SortOrder[] expectedOrdering =
                new SortOrder[] {
                        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
                        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
                };

        checkWriteDistributionAndOrdering(table, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
    }

    // =============================================================
    // Distribution and ordering for copy-on-write MERGE operations
    // =============================================================
    //
    // PARTITIONED BY date, days(ts) UNORDERED
    // -------------------------------------------------------------------------
    // merge mode is NOT SET -> unspecified distribution + LOCALLY ORDERED BY date, days(ts)
    //
    // PARTITIONED BY date ORDERED BY id
    // -------------------------------------------------------------------------
    // merge mode is NOT SET -> ORDERED BY date, id
    @Test
    public void testDefaultCopyOnWriteMergePartitionedUnsortedTable() {
        sql(
                "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
                        + "USING iceberg "
                        + "PARTITIONED BY (date, days(ts))",
                tableName);

        Table table = validationCatalog.loadTable(tableIdent);

        SortOrder[] expectedOrdering =
                new SortOrder[] {
                        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
                        Expressions.sort(Expressions.days("ts"), SortDirection.ASCENDING)
                };

        checkCopyOnWriteDistributionAndOrdering(table, MERGE, UNSPECIFIED_DISTRIBUTION, expectedOrdering);
    }

    @Test
    public void testDefaultCopyOnWriteMergePartitionedSortedTable() {
        sql(
                "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
                        + "USING iceberg "
                        + "PARTITIONED BY (date)", //Partitioned by date
                tableName);

        Table table = validationCatalog.loadTable(tableIdent);

        table.replaceSortOrder().desc("id").commit(); //Ordered by id DESC

        SortOrder[] expectedOrdering =
                new SortOrder[] {
                        Expressions.sort(Expressions.column("date"), SortDirection.ASCENDING),
                        Expressions.sort(Expressions.column("id"), SortDirection.DESCENDING)
                };

        Distribution expectedDistribution = Distributions.ordered(expectedOrdering);

        checkCopyOnWriteDistributionAndOrdering(table, MERGE, expectedDistribution, expectedOrdering);
    }

    private void checkWriteDistributionAndOrdering(
            Table table, Distribution expectedDistribution, SortOrder[] expectedOrdering) {
        SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
        DistributionMode distributionMode = writeConf.distributionMode();
        Distribution distribution =
                SparkDistributionAndOrderingUtil.buildRequiredDistribution(table, distributionMode);
        Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

        SortOrder[] ordering =
                SparkDistributionAndOrderingUtil.buildRequiredOrdering(table, distribution);
        Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
    }

    private void checkCopyOnWriteDistributionAndOrdering(
            Table table,
            RowLevelOperation.Command command,
            Distribution expectedDistribution,
            SortOrder[] expectedOrdering) {
        SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());

        DistributionMode mode = copyOnWriteDistributionMode(command, writeConf);

        Distribution distribution =
                SparkDistributionAndOrderingUtil.buildCopyOnWriteDistribution(table, command, mode);
        Assert.assertEquals("Distribution must match", expectedDistribution, distribution);

        SortOrder[] ordering =
                SparkDistributionAndOrderingUtil.buildCopyOnWriteOrdering(table, command, distribution);
        Assert.assertArrayEquals("Ordering must match", expectedOrdering, ordering);
    }

    private DistributionMode copyOnWriteDistributionMode(RowLevelOperation.Command command, SparkWriteConf writeConf) {
        switch (command) {
            case MERGE:
                return writeConf.copyOnWriteMergeDistributionMode();
            // Leaving other commands unimplemented to avoid implicit assumptions
            case DELETE:
            case UPDATE:
            default:
                throw new IllegalArgumentException("Unexpected command: " + command);
        }
    }
}
