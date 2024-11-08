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

package com.netflix.iceberg.metacat;

import org.apache.iceberg.*;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestMetacatClientOps {
    @Test
    public void testSortOrderFromString() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get()),
                Types.NestedField.required(3, "timestamp", Types.TimestampType.withoutZone()),
                Types.NestedField.required(4, "date", Types.DateType.get()),
                Types.NestedField.required(5, "decimal", Types.DecimalType.of(10, 2))
        );
        String sortOrderStr = "bucket(4, id) ASC NULLS FIRST, truncate(3, data) DESC NULLS LAST, year(date) ASC NULLS FIRST, month(timestamp) DESC NULLS LAST, decimal DESC NULLS FIRST";
        SortOrder sortOrder = MetacatClientOps.sortOrderFromString(schema, sortOrderStr);

        List<SortField> sortFields = sortOrder.fields();

        SortField bucketField = sortFields.get(0);
        Assert.assertEquals(Transforms.bucket(4), bucketField.transform());
        Assert.assertEquals(SortDirection.ASC, bucketField.direction());
        Assert.assertEquals(NullOrder.NULLS_FIRST, bucketField.nullOrder());
        Assert.assertEquals(1, bucketField.sourceId());
        Assert.assertEquals("bucket[4](1) ASC NULLS FIRST", bucketField.toString());

        SortField truncateField = sortFields.get(1);
        Assert.assertEquals(Transforms.truncate(3), truncateField.transform());
        Assert.assertEquals(SortDirection.DESC, truncateField.direction());
        Assert.assertEquals(NullOrder.NULLS_LAST, truncateField.nullOrder());
        Assert.assertEquals(2, truncateField.sourceId());
        Assert.assertEquals("truncate[3](2) DESC NULLS LAST", truncateField.toString());

        SortField yearField = sortFields.get(2);
        Assert.assertEquals(Transforms.year(), yearField.transform());
        Assert.assertEquals(SortDirection.ASC, yearField.direction());
        Assert.assertEquals(NullOrder.NULLS_FIRST, yearField.nullOrder());
        Assert.assertEquals(4, yearField.sourceId());
        Assert.assertEquals("year(4) ASC NULLS FIRST", yearField.toString());

        SortField monthField = sortFields.get(3);
        Assert.assertEquals(Transforms.month(), monthField.transform());
        Assert.assertEquals(SortDirection.DESC, monthField.direction());
        Assert.assertEquals(NullOrder.NULLS_LAST, monthField.nullOrder());
        Assert.assertEquals(3, monthField.sourceId());
        Assert.assertEquals("month(3) DESC NULLS LAST", monthField.toString());

        SortField identityField = sortFields.get(4);
        Assert.assertEquals(Transforms.identity(), identityField.transform());
        Assert.assertEquals(SortDirection.DESC, identityField.direction());
        Assert.assertEquals(NullOrder.NULLS_FIRST, identityField.nullOrder());
        Assert.assertEquals(5, identityField.sourceId());
        Assert.assertEquals("identity(5) DESC NULLS FIRST", identityField.toString());

    }
}
