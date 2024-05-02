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
package org.apache.iceberg.spark.procedures;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.actions.NfCloneTableSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

class NfCloneTableProcedure extends BaseProcedure {
    private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
            ProcedureParameter.required("source_table", DataTypes.StringType),
            ProcedureParameter.required("clone_table", DataTypes.StringType),
            ProcedureParameter.optional("additional_properties", STRING_MAP),
            ProcedureParameter.optional("include_snapshots", DataTypes.BooleanType),
    };

    private static final StructType OUTPUT_TYPE = new StructType(
            new StructField[]{
                    new StructField("cloned_files_count", DataTypes.LongType, false, Metadata.empty())
            }
    );

    private NfCloneTableProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    public static SparkProcedures.ProcedureBuilder builder() {
        return new BaseProcedure.Builder<NfCloneTableProcedure>() {
            @Override
            protected NfCloneTableProcedure doBuild() {
                return new NfCloneTableProcedure(tableCatalog());
            }
        };
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        String source = args.getString(0);
        Preconditions.checkArgument(source != null && !source.isEmpty(), "Source table cannot be null or empty");

        String clone = args.getString(1);
        Preconditions.checkArgument(clone != null && !clone.isEmpty(), "Clone table cannot be null or empty");
        Preconditions.checkArgument(!source.equals(clone), "Source table and clone table cannot be the same");

        Map<String, String> properties = Maps.newHashMap();
        if (!args.isNullAt(2)) {
            args.getMap(2)
                    .foreach(
                            DataTypes.StringType,
                            DataTypes.StringType,
                            (k, v) -> {
                                properties.put(k.toString(), v.toString());
                                return BoxedUnit.UNIT;
                            });
        }

        boolean includeSnapshot = !args.isNullAt(3) && args.getBoolean(3);

        NfCloneTableSparkAction action = SparkActions.get().cloneTable(source, clone, properties, includeSnapshot);

        NfCloneTableSparkAction.Result result = action.execute();

        return new InternalRow[]{newInternalRow(result.clonedDataFilesCount())};
    }

    @Override
    public String description() {
        return "NfCloneTableProcedure";
    }
}
