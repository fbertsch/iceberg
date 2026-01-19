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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSchemaUndelete extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestSchemaUndelete(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testUndeleteTopLevelColumn() {
    // Add a column, then delete it, then undelete it
    table.updateSchema().addColumn("count", Types.LongType.get(), "a count column").commit();

    int originalFieldId = table.schema().findField("count").fieldId();
    Assert.assertNotNull("Column should exist after adding", table.schema().findField("count"));

    // Delete the column
    table.updateSchema().deleteColumn("count").commit();
    Assert.assertNull("Column should not exist after deletion", table.schema().findField("count"));

    // Undelete the column
    table.updateSchema().undeleteColumn("count").commit();

    Types.NestedField restoredField = table.schema().findField("count");
    Assert.assertNotNull("Column should exist after undelete", restoredField);
    Assert.assertEquals(
        "Field ID should be preserved", originalFieldId, restoredField.fieldId());
    Assert.assertEquals(
        "Field type should be preserved", Types.LongType.get(), restoredField.type());
    Assert.assertEquals("Field doc should be preserved", "a count column", restoredField.doc());
  }

  @Test
  public void testUndeleteNestedField() {
    // Add a struct with nested fields
    table
        .updateSchema()
        .addColumn(
            "location",
            Types.StructType.of(
                Types.NestedField.optional(100, "lat", Types.DoubleType.get()),
                Types.NestedField.optional(101, "long", Types.DoubleType.get())))
        .commit();

    int latFieldId = table.schema().findField("location.lat").fieldId();
    Assert.assertNotNull("Nested field should exist", table.schema().findField("location.lat"));

    // Delete the nested field
    table.updateSchema().deleteColumn("location.lat").commit();
    Assert.assertNull(
        "Nested field should not exist after deletion", table.schema().findField("location.lat"));

    // Undelete the nested field
    table.updateSchema().undeleteColumn("location.lat").commit();

    Types.NestedField restoredField = table.schema().findField("location.lat");
    Assert.assertNotNull("Nested field should exist after undelete", restoredField);
    Assert.assertEquals("Field ID should be preserved", latFieldId, restoredField.fieldId());
    Assert.assertEquals(
        "Field type should be preserved", Types.DoubleType.get(), restoredField.type());
  }

  @Test
  public void testUndeleteColumnAlreadyExists() {
    // Try to undelete a column that already exists
    Assertions.assertThatThrownBy(() -> table.updateSchema().undeleteColumn("id").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already exists in the current schema");
  }

  @Test
  public void testUndeleteColumnNotFound() {
    // Try to undelete a column that was never in the schema
    Assertions.assertThatThrownBy(
            () -> table.updateSchema().undeleteColumn("nonexistent_column").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found in any historical schema");
  }

  @Test
  public void testUndeletePreservesFieldId() {
    // This test explicitly verifies that the field ID is preserved (not a new ID)
    table.updateSchema().addColumn("temp_col", Types.StringType.get()).commit();

    int originalId = table.schema().findField("temp_col").fieldId();

    // Add another column to increment the lastColumnId
    table.updateSchema().addColumn("another_col", Types.IntegerType.get()).commit();
    int lastIdAfterAdd = table.schema().findField("another_col").fieldId();

    // Delete temp_col
    table.updateSchema().deleteColumn("temp_col").commit();

    // Undelete temp_col
    table.updateSchema().undeleteColumn("temp_col").commit();

    Types.NestedField restored = table.schema().findField("temp_col");
    Assert.assertEquals(
        "Restored field should have original ID, not a new one", originalId, restored.fieldId());
    Assert.assertTrue(
        "Restored field ID should be less than the last assigned ID",
        restored.fieldId() < lastIdAfterAdd);
  }

  @Test
  public void testUndeleteNestedFieldParentMissing() {
    // Add a struct, delete the whole struct, then try to undelete a nested field
    table
        .updateSchema()
        .addColumn(
            "prefs",
            Types.StructType.of(
                Types.NestedField.optional(200, "setting1", Types.BooleanType.get()),
                Types.NestedField.optional(201, "setting2", Types.BooleanType.get())))
        .commit();

    // Delete the entire parent struct
    table.updateSchema().deleteColumn("prefs").commit();
    Assert.assertNull("Parent struct should not exist", table.schema().findField("prefs"));

    // Try to undelete nested field when parent doesn't exist
    Assertions.assertThatThrownBy(
            () -> table.updateSchema().undeleteColumn("prefs.setting1").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("parent struct")
        .hasMessageContaining("does not exist")
        .hasMessageContaining("Undelete the parent first");
  }

  @Test
  public void testUndeleteParentThenNestedField() {
    // Add a struct, delete the whole struct, then undelete parent, then undelete nested field
    table
        .updateSchema()
        .addColumn(
            "config",
            Types.StructType.of(
                Types.NestedField.optional(300, "enabled", Types.BooleanType.get()),
                Types.NestedField.optional(301, "value", Types.StringType.get())))
        .commit();

    int enabledId = table.schema().findField("config.enabled").fieldId();
    int configId = table.schema().findField("config").fieldId();

    // Delete both nested fields to empty the struct, then delete the struct
    table.updateSchema().deleteColumn("config.enabled").deleteColumn("config.value").commit();
    table.updateSchema().deleteColumn("config").commit();

    Assert.assertNull("Parent struct should not exist", table.schema().findField("config"));

    // Undelete the parent struct first
    table.updateSchema().undeleteColumn("config").commit();

    Types.NestedField restoredConfig = table.schema().findField("config");
    Assert.assertNotNull("Parent struct should exist after undelete", restoredConfig);
    Assert.assertEquals("Parent struct field ID should be preserved", configId, restoredConfig.fieldId());

    // Now undelete the nested field
    table.updateSchema().undeleteColumn("config.enabled").commit();

    Types.NestedField restoredEnabled = table.schema().findField("config.enabled");
    Assert.assertNotNull("Nested field should exist after undelete", restoredEnabled);
    Assert.assertEquals("Nested field ID should be preserved", enabledId, restoredEnabled.fieldId());
  }

  @Test
  public void testUndeleteRequiredColumnBecomesOptional() {
    // Add a required column, delete it, then undelete it
    // The undeleted column should be optional because new data may have been written
    // without this column after it was deleted
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();

    Types.NestedField originalField = table.schema().findField("required_col");
    Assert.assertTrue("Column should be required initially", originalField.isRequired());
    int originalFieldId = originalField.fieldId();

    // Delete the required column
    table.updateSchema().deleteColumn("required_col").commit();
    Assert.assertNull(
        "Column should not exist after deletion", table.schema().findField("required_col"));

    // Undelete the column - it should now be optional
    table.updateSchema().undeleteColumn("required_col").commit();

    Types.NestedField restoredField = table.schema().findField("required_col");
    Assert.assertNotNull("Column should exist after undelete", restoredField);
    Assert.assertEquals("Field ID should be preserved", originalFieldId, restoredField.fieldId());
    Assert.assertTrue(
        "Undeleted column must be optional (not required) because new data may have been "
            + "written without this column",
        restoredField.isOptional());
  }

  @Test
  public void testUndeleteCaseInsensitive() {
    // Add and delete a column
    table.updateSchema().addColumn("MixedCase", Types.StringType.get()).commit();
    int originalId = table.schema().findField("MixedCase").fieldId();
    table.updateSchema().deleteColumn("MixedCase").commit();

    // Undelete with different case (case insensitive mode)
    table.updateSchema().caseSensitive(false).undeleteColumn("mixedcase").commit();

    Types.NestedField restored = table.schema().findField("MixedCase");
    Assert.assertNotNull("Column should be restored with case-insensitive undelete", restored);
    Assert.assertEquals("Field ID should be preserved", originalId, restored.fieldId());
  }
}
