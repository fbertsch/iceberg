package org.apache.iceberg.spark.procedures;

import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import com.netflix.bdp.sparkextensions.*;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;

public class GetTablesProcedure extends BaseProcedure {
    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[]{
                    ProcedureParameter.required("query_or_view", DataTypes.StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[]{
                            new StructField("table_or_view_name", DataTypes.StringType, true, Metadata.empty()),
                            new StructField("is_table", DataTypes.BooleanType, true, Metadata.empty())
                    });

    private GetTablesProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<GetTablesProcedure>() {
            @Override
            protected GetTablesProcedure doBuild() {
                return new GetTablesProcedure(tableCatalog());
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
        String queryOrView = args.getString(0);
        SparkSession spark = SparkSession.active();
        ArrayList<InternalRow> rows = new ArrayList<>();

        try {
            scala.collection.Seq<Tuple2<String, Object>> scalaTableAndViewNames = GetTableProvider.getTablesFromLogicalPlan(spark, queryOrView);
            List<Tuple2<String, Object>> tableAndViewNames = JavaConverters.seqAsJavaList(scalaTableAndViewNames);

            for (Tuple2<String, Object> entry : tableAndViewNames) {
                String tableName = entry._1();
                Boolean isTable = (Boolean) entry._2(); // Explicitly cast to Boolean
                InternalRow row = new GenericInternalRow(new Object[]{UTF8String.fromString(tableName), isTable});
                rows.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse SQL query or view: " + queryOrView, e);
        }

        return rows.toArray(new InternalRow[0]);
    }

    @Override
    public String description() {
        return "GetTables";
    }
}