package com.netflix.iceberg.metacat;

import de.huxhorn.sulky.ulid.ULID;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30)
public class TableTest {

  private Catalog catalog;

  private String catalogName;
  private String dbName;
  private String uniqueTableName;

  private TableIdentifier testTableIdentifier;

  @BeforeEach
  void setup(TestInfo testInfo) {
    Configuration conf = new Configuration(false);
    conf.addResource(TableTest.class.getResourceAsStream("/hadoop/core-site.xml"));
    catalog = new MetacatIcebergCatalog(conf, "iceberg-client-integration-test");
    catalogName = "testhive";
    // Janitor purges tables in this database
    // https://manuals.netflix.net/view/janitor-docs/mkdocs/master/datahygiene/
    dbName = "bdp_integration_tests";
    uniqueTableName = getUniqueTableName(testInfo);
  }

  private static String getUniqueTableName(TestInfo testInfo) {
    String testClass = testInfo.getTestClass()
        .map(Class::getSimpleName)
        .map(String::toLowerCase)
        .orElse("unknown");
    String testMethod = testInfo.getTestMethod()
        .map(Method::getName)
        .map(String::toLowerCase)
        .orElse("unknown");
    String run_id = new ULID().nextULID().toLowerCase();
    return String.format("%s_%s_%s", testClass, testMethod, run_id);
  }

  private Table createTestTable(Schema schema, PartitionSpec spec) {
    testTableIdentifier = TableIdentifier.of(catalogName, dbName, uniqueTableName);
    System.err.println("Creating table " + testTableIdentifier);
    return catalog.createTable(testTableIdentifier, schema, spec);
  }

  @AfterEach
  void dropTableTable() {
    if (testTableIdentifier != null) {
      System.err.println("Dropping table " + testTableIdentifier);
      catalog.dropTable(testTableIdentifier);
    }
  }

  @Test
  public void testRead() {
    TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, "iceberg", "secure_dual");
    printTable(catalog.loadTable(tableIdentifier));
  }

  @Test
  public void testReadUnsecure() {
    TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, "iceberg", "dual");
    printTable(catalog.loadTable(tableIdentifier));
  }

  /**
   * Try to commit a branch to a table. Fail if branching is disabled in MetacatClientOps.java
   * @throws IOException
   */
  @Test void testDisableBranching() throws IOException {
    // Create a new table
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

    Record record = GenericRecord.create(schema);
    record.setField("id", 10L);

    Table table = createTestTable(schema, PartitionSpec.unpartitioned());

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
    // Try to commit a fresh branch
    boolean COMMIT_FAILED = false;
    try{
      table.manageSnapshots().createBranch("test", table.currentSnapshot().snapshotId()).commit();
    } catch (Exception e){
      e.printStackTrace();
      COMMIT_FAILED = true;
    }
    Assert.assertTrue("Branch creation should be disabled", COMMIT_FAILED);
  }

  @Test
  public void testUnpartitionedWrite() throws IOException {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

    Record record = GenericRecord.create(schema);
    record.setField("id", 10L);

    Table table = createTestTable(schema, PartitionSpec.unpartitioned());

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

    Table table = createTestTable(schema, spec);

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

    Table table = createTestTable(schema, spec);

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
}
