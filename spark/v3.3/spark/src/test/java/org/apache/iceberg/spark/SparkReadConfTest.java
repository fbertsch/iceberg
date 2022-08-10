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
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.Assert.*;

public class SparkReadConfTest extends SparkTestBase {

    Table table = Mockito.mock(Table.class);

    @Test
    public void splitSizeFromOption() {
        long splitSize = 12345;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSize));
        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals(splitSize, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromConf() {
        long splitSize = 67890000;
        Mockito.when(table.name()).thenReturn("db.tbl");
        spark.conf().set("spark.netflix.db.tbl.target-size", Long.toString(splitSize));
        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(splitSize, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeFromBoth() {
        long splitSizeOption = 12345;
        long splitSizeConf = 67890000;
        Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, Long.toString(splitSizeOption));
        Mockito.when(table.name()).thenReturn("db.tbl");
        spark.conf().set("spark.netflix.db.tbl.target-size", Long.toString(splitSizeConf));
        SparkReadConf readConf = new SparkReadConf(spark, table, options);
        Assert.assertEquals("Read option overrides Spark config",
                splitSizeOption, readConf.splitSizeOption().longValue());
    }

    @Test
    public void splitSizeNone() {
        SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
        Assert.assertEquals(null, readConf.splitSizeOption());
    }
}