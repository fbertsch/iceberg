package com.netflix.iceberg.metacat;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class TableTest {

  private Catalog catalog;

  private TableIdentifier tableIdentifier;

  @BeforeEach
  void setup(TestInfo testInfo) {
    Configuration conf = new Configuration();
    conf.addResource(TableTest.class.getResourceAsStream("/hadoop/core-site.xml"));
    catalog = new MetacatIcebergCatalog(conf, "iceberg-client-integration-test");
    tableIdentifier = testTableIdentifier(testInfo);
  }

  @AfterEach
  void shutdown() {
    catalog.dropTable(tableIdentifier);
  }

  @Test
  public void testReadUnsecure() {
    TableIdentifier tableIdentifier = TableIdentifier.of("prodhive", "iceberg", "dual");
    Table table = catalog.loadTable(tableIdentifier);
    printTable(table);
  }

  @Test
  public void testRead() {
    TableIdentifier tableIdentifier = TableIdentifier.of("prodhive", "iceberg", "secure_dual");
    Table table = catalog.loadTable(tableIdentifier);
    printTable(table);
  }

  @Test
  public void testUnpartitionedWrite() throws IOException {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

    Record record = GenericRecord.create(schema);
    record.setField("id", 10L);

    catalog.createTable(tableIdentifier, schema);
    Table table = catalog.loadTable(tableIdentifier);

    String fileName = String.format("%s.parquet", UUID.randomUUID());
    String dataLocation = table.locationProvider().newDataLocation(fileName);
    OutputFile outputFile = table.io().newOutputFile(dataLocation);

    FileAppender<Record> writer = Parquet.write(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build();
    try {
      writer.add(record);
    } finally {
      writer.close();
    }

    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(outputFile.toInputFile())
        .withFileSizeInBytes(writer.length())
        .withMetrics(writer.metrics())
        .withSplitOffsets(writer.splitOffsets())
        .build();

    table.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())  // Replace entire table
        .addFile(dataFile)
        .commit();

    printTable(table);
  }

  @Test
  public void testPartitionedWrite() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "dateint", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    int currentKey = 20200221;

    Record record = GenericRecord.create(schema);
    record.setField("dateint", currentKey);
    record.setField("data", "foo");

    Record partition = GenericRecord.create(spec.partitionType());
    partition.setField("dateint", currentKey);

    catalog.createTable(tableIdentifier, schema, spec);
    Table table = catalog.loadTable(tableIdentifier);

    String fileName = String.format("%s.parquet", UUID.randomUUID());
    String dataLocation = table.locationProvider().newDataLocation(
        table.spec(), partition, fileName);
    OutputFile outputFile = table.io().newOutputFile(dataLocation);

    FileAppender<Record> writer = Parquet.write(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build();
    try {
      writer.add(record);
    } finally {
      writer.close();
    }

    DataFile dataFile = DataFiles.builder(spec)
        .withPartition(partition)
        .withInputFile(outputFile.toInputFile())
        .withFileSizeInBytes(writer.length())
        .withMetrics(writer.metrics())
        .withSplitOffsets(writer.splitOffsets())
        .build();

    table.newOverwrite()
        .overwriteByRowFilter(Expressions.equal("dateint", currentKey))  // Replace a partition
        .addFile(dataFile)
        .commit();

    printTable(table);
  }

  @Test
  public void testPartitionedWriteWithTransform() throws IOException {
    // Spark only supports timestamp with timezone
    Types.TimestampType tsType = Types.TimestampType.withZone();
    Schema schema = new Schema(
        Types.NestedField.optional(1, "ts", tsType),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();

    Record record = GenericRecord.create(schema);
    OffsetDateTime ts = OffsetDateTime.parse("2020-02-21T10:12:00.038194Z");
    record.setField("ts", ts);
    record.setField("data", "foo");

    Transform<Long, Integer> day = Transforms.day(tsType);
    // All time and timestamp values are stored with microsecond precision.
    Integer tsDay = day.apply(toEpochMicros(ts));

    Record partition = GenericRecord.create(spec.partitionType());
    partition.setField("ts_day", tsDay);

    catalog.createTable(tableIdentifier, schema, spec);
    Table table = catalog.loadTable(tableIdentifier);

    String fileName = String.format("%s.parquet", UUID.randomUUID());
    String dataLocation = table.locationProvider().newDataLocation(
        table.spec(), partition, fileName);
    OutputFile outputFile = table.io().newOutputFile(dataLocation);

    FileAppender<Record> writer = Parquet.write(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build();
    try {
      writer.add(record);
    } finally {
      writer.close();
    }

    DataFile dataFile = DataFiles.builder(spec)
        .withPartition(partition)
        .withInputFile(outputFile.toInputFile())
        .withFileSizeInBytes(writer.length())
        .withMetrics(writer.metrics())
        .withSplitOffsets(writer.splitOffsets())
        .build();

    table.newOverwrite()
        .overwriteByRowFilter(
            Expressions.lessThan("ts", toEpochMicros(ts.toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC))))
        .addFile(dataFile)
        .commit();

    printTable(table);
  }

  private Long toEpochMicros(OffsetDateTime ts) {
    return toEpochMicros(ts.toInstant());
  }

  private Long toEpochMicros(Instant instant) {
    return TimeUnit.MILLISECONDS.toMicros(instant.toEpochMilli()) +
        TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  private void printTable(Table table) {
    table.refresh();
    for (Record r : IcebergGenerics.read(table).reuseContainers().build()) {
      System.out.println(r.toString());
    }
  }

  private static TableIdentifier testTableIdentifier(TestInfo testInfo) {
    return TableIdentifier.of("prodhive", "bdp_integration_tests", uniqueTableName(testInfo));
  }

  private static String uniqueTableName(TestInfo testInfo) {
    String className = testInfo.getTestClass().map(Class::getSimpleName).orElse("Unknown");
    String methodName = testInfo.getTestMethod().map(Method::getName).orElse("unknown");
    long timestamp = System.currentTimeMillis();
    return String.format("%s_%s_%d", className, methodName, timestamp);
  }
}
