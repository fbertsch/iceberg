/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SparkReadConfTest {

    private SparkSession spark;
    private Table table;

    @Before
    public void Setup() {
        RuntimeConfig conf = Mockito.mock(RuntimeConfig.class);
        Mockito.when(conf.get(Mockito.anyString(), Mockito.anyString()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        spark = Mockito.mock(SparkSession.class);
        Mockito.when(spark.conf()).thenReturn(conf);
        table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("cat.db.tbl");
    }

    @Test
    public void splitSizeDefault() {
        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(TableProperties.SPLIT_SIZE_DEFAULT, readConf.splitSize());
        Assert.assertEquals(null, readConf.splitSizeOption());
    }

    @Test
    public void splitSizeMaxPartitionBytes() {
        long maxPartitionBytes = 67890000;
        Mockito.when(spark.conf().get(SQLConf.FILES_MAX_PARTITION_BYTES().key(), null))
            .thenReturn(String.valueOf(maxPartitionBytes));

        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertNotEquals(maxPartitionBytes, TableProperties.SPLIT_SIZE_DEFAULT);
        Assert.assertEquals(maxPartitionBytes, readConf.splitSize());
        Assert.assertEquals(null, readConf.splitSizeOption());
    }

    @Test
    public void splitSizeFromTableProp() {
        long splitSizeTableProp = 7654;
        Mockito.when(table.properties())
            .thenReturn(ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(splitSizeTableProp, readConf.splitSize());
        Assert.assertEquals(null, readConf.splitSizeOption());
    }

    @Test
    public void splitSizeFromOption() {
        long splitSizeOption = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));

        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals(splitSizeOption, readConf.splitSize());
        Assert.assertEquals(splitSizeOption, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromConf() {
        long splitSizeConf = 67890000;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.target-size", null))
            .thenReturn(String.valueOf(splitSizeConf));

        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(splitSizeConf, readConf.splitSize());
        Assert.assertEquals(splitSizeConf, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromBothOptionAndConf() {
        long splitSizeOption = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));

        long splitSizeConf = 67890000;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.target-size", null))
            .thenReturn(String.valueOf(splitSizeConf));

        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals("Read option overrides Spark config", splitSizeOption, readConf.splitSize());
        Assert.assertEquals("Read option overrides Spark config",
            splitSizeOption, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromBothOptionAndTableProp() {
        long splitSizeOption = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));

        long splitSizeTableProp = 7654;
        Mockito.when(table.properties())
            .thenReturn(ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals("Read option overrides table property", splitSizeOption, readConf.splitSize());
        Assert.assertEquals("Read option overrides table property",
            splitSizeOption, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromBothConfAndTableProp() {
        long splitSizeConf = 67890000;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.target-size", null))
            .thenReturn(String.valueOf(splitSizeConf));

        long splitSizeTableProp = 7654;
        Mockito.when(table.properties())
            .thenReturn(ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals("Spark config overrides table property", splitSizeConf, readConf.splitSize());
        Assert.assertEquals("Spark config overrides table property",
            splitSizeConf, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromDefaultConfAndTableProp() {
        long maxPartitionBytes = 67890000;
        Mockito.when(spark.conf().get(SQLConf.FILES_MAX_PARTITION_BYTES().key(), null))
            .thenReturn(String.valueOf(maxPartitionBytes));

        long splitSizeTableProp = 7654;
        Mockito.when(table.properties())
            .thenReturn(ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(
            "Table property over spark.sql.files.maxPartitionBytes",
            splitSizeTableProp,
            readConf.splitSize());
        Assert.assertEquals(null, readConf.splitSizeOption());
    }

    @Test
    public void splitSizeFromAll() {
        long splitSizeOption = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));

        long splitSizeConf = 67890000;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.target-size", null))
            .thenReturn(String.valueOf(splitSizeConf));

        long splitSizeTableProp = 7654;
        Mockito.when(table.properties())
            .thenReturn(ImmutableMap.of(TableProperties.SPLIT_SIZE, String.valueOf(splitSizeTableProp)));

        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals("Read option overrides all others", splitSizeOption, readConf.splitSize());
        Assert.assertEquals("Read option overrides all others",
            splitSizeOption, readConf.splitSizeOption().longValue());
    }

    @Test
    public void snapshotIdNone() {
        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertNull(readConf.snapshotId());
        Assert.assertNull(readConf.snapshotId());
    }

    @Test
    public void snapshotIdFromOption() {
        long snapshotIdOption = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, Long.toString(snapshotIdOption));

        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals(snapshotIdOption, readConf.snapshotId().longValue());
    }

    @Test
    public void snapshotIdFromConf() {
        long snapshotIdConf = 54321;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.snapshot-id", null))
            .thenReturn(String.valueOf(snapshotIdConf));

        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(snapshotIdConf, readConf.snapshotId().longValue());
    }

    @Test
    public void snapshotIdFromConfAndOption() {
        long snapshotIdOption = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, Long.toString(snapshotIdOption));

        long snapshotIdConf = 54321;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.snapshot-id", null))
            .thenReturn(String.valueOf(snapshotIdConf));

        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals("Read option overrides Spark config", snapshotIdOption, readConf.snapshotId().longValue());
    }

    @Test
    public void multipleSnapshotIdsFromConf() {
        long firstSnapshotIdConf = 54321;
        long secondSnapshotIdConf = 12345;
        Mockito.when(spark.conf().get("spark.netflix.db.tbl.snapshot-id", null))
            .thenReturn(String.valueOf(firstSnapshotIdConf));
        Mockito.when(spark.conf().get("spark.netflix.db.tbl2.snapshot-id", null))
            .thenReturn(String.valueOf(secondSnapshotIdConf));

        SparkReadConf firstReadConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(firstSnapshotIdConf, firstReadConf.snapshotId().longValue());

        Table secondTable = Mockito.mock(Table.class);
        Mockito.when(secondTable.name()).thenReturn("cat.db.tbl2");
        SparkReadConf secondReadConf = new SparkReadConf(spark, secondTable, ImmutableMap.of());
        Assert.assertEquals(secondSnapshotIdConf, secondReadConf.snapshotId().longValue());
    }
}