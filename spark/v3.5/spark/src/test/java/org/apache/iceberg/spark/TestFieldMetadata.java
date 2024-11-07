/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFieldMetadata extends SparkCatalogTestBase {

  @Parameterized.Parameters(name = "catalog={0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"testhive", SparkCatalog.class.getName(), ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "cache-enabled", "false"  // Do not cache table object
        )}
    };
  }

  private static final Metadata longMetadata = new MetadataBuilder().putLong("longKey", 3).build();

  private static final Metadata doubleMetadata = new MetadataBuilder().putDouble("doubleKey", 3.5D).build();

  private static final Metadata booleanMetadata = new MetadataBuilder().putBoolean("booleanKey", true).build();

  private static final Metadata stringMetadata = new MetadataBuilder().putString("stringKey", "stringValue").build();

  private static final Metadata longArrayMetadata = new MetadataBuilder()
      .putLongArray("longArrayKey", new long[] {3, 5, 7})
      .build();

  private static final Metadata doubleArrayMetadata = new MetadataBuilder()
      .putDoubleArray("doubleArrayKey", new double[] {3.5D, 100.67D})
      .build();

  private static final Metadata booleanArrayMetadata = new MetadataBuilder()
      .putBooleanArray("booleanArrayKey", new boolean[] {true, false, true})
      .build();

  private static final Metadata stringArrayMetadata = new MetadataBuilder()
      .putStringArray("stringArrayKey", new String[] {"v1", "v2", "v3", "v4"})
      .build();

  private static final Metadata nestedMetadata = new MetadataBuilder()
      .putString("stringKey", "stringValue")
      .putMetadata("metadataKey", doubleMetadata)
      .build();

  private static final Metadata nestedMetadata2 = new MetadataBuilder()
      .putMetadata("metadataKey2", nestedMetadata)
      .putBoolean("booleanKey", true)
      .build();

  private static final Metadata nestedMetadataArray = new MetadataBuilder()
      .putMetadataArray(
          "metadataArrayKey",
          new Metadata[] {
              stringMetadata,
              doubleMetadata,
              nestedMetadata2
          })
      .build();

  private static final Metadata nestedMetadataArray2 = new MetadataBuilder()
      .putMetadata("nestedMetadataArrayKey", nestedMetadataArray)
      .putString("stringKey", "stringValue")
      .putMetadata("booleanArrayMetadataKey", booleanArrayMetadata)
      .build();

  public TestFieldMetadata(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testSimple() throws Exception {
    verifyFieldMetadata(longMetadata, doubleMetadata, booleanMetadata, stringMetadata);
  }

  @Test
  public void testArray() throws Exception {
    verifyFieldMetadata(longArrayMetadata, doubleArrayMetadata, booleanArrayMetadata, stringArrayMetadata);
  }

  @Test
  public void testNested() throws Exception {
    verifyFieldMetadata(booleanMetadata, nestedMetadata2);
  }

  @Test
  public void testNestedArray() throws Exception {
    verifyFieldMetadata(nestedMetadataArray2, nestedMetadata, stringArrayMetadata, longMetadata);
  }

  private void verifyFieldMetadata(Metadata... expectedMetadataArray) throws Exception {
    StructType expectedSchema = buildSchema(expectedMetadataArray);
    StructType actualSchema = saveAndLoadSchema(expectedSchema);
    for (StructField field : expectedSchema.fields()) {
      Assert.assertEquals(field.metadata(), actualSchema.apply(field.name()).metadata());
    }
  }

  private StructType saveAndLoadSchema(StructType schema) throws Exception {
    String tableName = UUID.randomUUID().toString().replaceAll("-", "");
    String[] tableNamespace = new String[] {"default"};
    Identifier identifier = Identifier.of(tableNamespace, tableName);
    TableCatalog catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    catalog.createTable(identifier, schema, new Transform[0], ImmutableMap.of());
    return catalog.loadTable(identifier).schema();
  }

  private static StructType buildSchema(Metadata... metadata) {
    StructField[] fields = Arrays.stream(metadata)
        .map(TestFieldMetadata::randomFieldWithMetadata)
        .toArray(StructField[]::new);
    return new StructType(fields);
  }

  private static StructField randomFieldWithMetadata(Metadata metadata) {
    return new StructField(UUID.randomUUID().toString(), DataTypes.StringType, true, metadata);
  }
}
