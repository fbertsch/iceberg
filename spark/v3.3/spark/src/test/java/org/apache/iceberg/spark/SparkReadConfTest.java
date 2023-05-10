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
    public void splitSizeNone() {
        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(TableProperties.SPLIT_SIZE_DEFAULT, readConf.splitSize());
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
}