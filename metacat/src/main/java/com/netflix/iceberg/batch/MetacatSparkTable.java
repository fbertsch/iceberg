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

package com.netflix.iceberg.batch;

import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE;
import static org.apache.spark.sql.connector.catalog.TableCapability.OVERWRITE_DYNAMIC;
import static org.apache.spark.sql.connector.catalog.TableCapability.TRUNCATE;

public class MetacatSparkTable implements Table, SupportsRead, SupportsWrite {
  private static final Logger LOG = LoggerFactory.getLogger(MetacatSparkTable.class);
  private static final Set<TableCapability> CAPABILITIES = Sets.newHashSet(
      BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC);
  private static final Set<TableCapability> UNPARTITIONED_CAPABILITIES = Sets.newHashSet(
      BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC, TRUNCATE);
  private static final Joiner DOT = Joiner.on('.');

  /**
   * Create a new Metacat TableDto object from Spark table metadata
   *
   * @param name a Metacat QualifiedName
   * @param schema a Spark StructType schema
   * @param partitioning Spark partition Transforms
   * @param properties a Map of string table properties
   * @param defaultLocation the default location for the table, if not set in properties
   * @return a TableDto
   */
  static TableDto createTableInfo(QualifiedName name, StructType schema, Transform[] partitioning,
                                  Map<String, String> properties, String defaultLocation) {
    String provider = SparkTables.provider(properties);
    String path = properties.getOrDefault(SparkTables.OPTION_PREFIX + SparkTables.PATH, defaultLocation);
    String location = properties.getOrDefault(SparkTables.LOCATION, path);

    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.putAll(SparkTables.toSparkProperties(schema, partitioning, provider));
    properties.entrySet().stream()
        .filter(entry -> !SparkTables.isReservedProperty(entry.getKey()))
        .forEach(propsBuilder::put);

    // set up the Spark table
    StorageDto serde = new StorageDto();
    serde.setUri(location);
    if (SparkTables.isSupportedFormat(provider)) {
      // STORED AS parquet, orc, and avro will use this branch and are converted to Spark implementations
      serde.setInputFormat(SparkTables.inputFormat(provider));
      serde.setOutputFormat(SparkTables.outputFormat(provider));
      serde.setSerializationLib(SparkTables.serde(provider));
      serde.setSerdeInfoParameters(SparkTables.options(properties, location));

    } else if (properties.containsKey(SparkTables.HIVE_STORED_AS)) {
      String storedAs = properties.get(SparkTables.HIVE_STORED_AS);
      String defaultInputFormat = SparkTables.defaultInputFormat(storedAs);
      String defaultOutputFormat = SparkTables.defaultOutputFormat(storedAs);
      String defaultSerde = SparkTables.defaultSerde(storedAs);

      serde.setInputFormat(properties.getOrDefault(SparkTables.HIVE_INPUT_FORMAT, defaultInputFormat));
      serde.setOutputFormat(properties.getOrDefault(SparkTables.HIVE_OUTPUT_FORMAT, defaultOutputFormat));
      serde.setSerializationLib(properties.getOrDefault(SparkTables.HIVE_SERDE, defaultSerde));
      serde.setSerdeInfoParameters(SparkTables.options(properties));

    } else {
      String defaultSerde = SparkTables.defaultSerde();

      serde.setInputFormat(properties.get(SparkTables.HIVE_INPUT_FORMAT));
      serde.setOutputFormat(properties.get(SparkTables.HIVE_OUTPUT_FORMAT));
      serde.setSerializationLib(properties.getOrDefault(SparkTables.HIVE_SERDE, defaultSerde));
      serde.setSerdeInfoParameters(SparkTables.options(properties));
    }

    TableDto newTableInfo = new TableDto();
    newTableInfo.setName(name);
    newTableInfo.setSerde(serde);
    newTableInfo.setDataExternal(true);
    newTableInfo.setMetadata(propsBuilder.build());
    newTableInfo.setFields(BatchUtil.toMetacatSchema(schema, partitioning));

    return newTableInfo;
  }

  private final SparkSession spark;
  private final Client client;
  private final String catalog;
  private final String database;
  private final String name;

  private TableDto table;
  private StructType lazySchema = null;
  private Transform[] lazyTransforms = null;
  private Map<String, String> lazyProperties = null;
  private Schema lazyIcebergSchema = null;
  private PartitionSpec lazySpec = null;
  private StructType lazyPartitionSchema = null;

  MetacatSparkTable(Client client, String catalog, String database, String name, TableDto table) {
    this.spark = SparkSession.active();
    this.client = client;
    // table.getFields().forEach(field -> field.isPartition_key());
    this.catalog = catalog;
    this.database = database;
    this.name = name;
    this.table = table;
  }

  public String catalog() {
    return catalog;
  }

  public String database() {
    return database;
  }

  public String table() {
    return name;
  }

  @Override
  public String name() {
    return DOT.join(catalog, database, name);
  }

  @Override
  public StructType schema() {
    if (lazySchema == null) {
      // if the table has Spark's schema properties, use those instead of Metacat's schema
      try {
        this.lazySchema = SparkTables.schema(table.getMetadata());
      } catch (RuntimeException e) {
        LOG.warn("Failed to load schema from Spark table properties; using Hive metadata", e);
      }

      StructType metacatSchema = convertSchema(spark, table);
      if (lazySchema == null) {
        this.lazySchema = metacatSchema;
      } else if (!BatchUtil.equalsIgnoreCaseAndNullability(lazySchema, metacatSchema)) {
        LOG.warn("The table schema given by Hive metastore is different from " +
                "the schema when this table was created by Spark SQL. " +
                "We have to fall back to the table schema from Hive metastore which is not case preserving.\n\n" +
                "Table name: {}\n\n" +
                "Schema from Hive metastore:\n{}\n" +
                "Schema when this table was created by Spark SQL:\n{}",
            name(), metacatSchema.treeString().replaceAll("(?m)^", "  "), lazySchema.treeString().replaceAll("(?m)^", "  "));
        this.lazySchema = metacatSchema;
      }
    }
    return lazySchema;
  }

  @Override
  public Transform[] partitioning() {
    if (lazyTransforms == null) {
      List<String> partitionColumnsSpark = null;
      try {
        partitionColumnsSpark = SparkTables.partitionColumns(table.getMetadata());
      } catch (RuntimeException e) {
        LOG.warn("Failed to load partitioning from Spark table properties; using Hive metadata", e);
      }

      List<String> partitionColumnsMetacat = partitionColumns(table);

      if (partitionColumnsSpark != null && !partitionColumnsSpark.equals(partitionColumnsMetacat)) {
        throw new IllegalArgumentException(
            "Partition columns in Spark table properties do not match those in Metacat: " + name());
      }

      this.lazyTransforms = partitionColumnsMetacat.stream()
          .map(Expressions::identity)
          .toArray(Transform[]::new);
    }
    return lazyTransforms;
  }

  @Override
  public Map<String, String> properties() {
    if (lazyProperties == null) {
      this.lazyProperties = SparkTables.properties(
          table.getMetadata(), provider(),
          table.getSerde().getInputFormat(), table.getSerde().getOutputFormat(),
          table.getSerde().getSerializationLib());
    }
    return lazyProperties;
  }

  String formatDescription() {
    return properties().get(SparkTables.FORMAT);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new MetacatScanBuilder(spark, this, options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new BatchPatternWriteBuilder(spark, this, info);
  }

  void updateTable(StructType struct, Map<String, String> newProperties, Set<String> removedProperties) {
    // apply changes to the metacat metadata, because table.properties() may be filtered
    // table properties are created using the Spark metadata, then existing properties, then new properties
    ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
    tableProperties.putAll(SparkTables.toSparkProperties(struct, partitioning(), table.getMetadata()));

    table.getMetadata().entrySet().stream()
        .filter(entry -> !removedProperties.contains(entry.getKey()))
        .filter(entry -> !newProperties.containsKey(entry.getKey()))
        .filter(entry -> !SparkTables.isReservedProperty(entry.getKey()))
        .forEach(tableProperties::put);

    newProperties.entrySet().stream()
        .filter(entry -> !SparkTables.isReservedProperty(entry.getKey()))
        .forEach(tableProperties::put);

    // update the table location if "location" was set
    newProperties.entrySet().stream()
        .filter(entry -> SparkTables.LOCATION.equals(entry.getKey()))
        .map(Map.Entry::getValue)
        .forEach(location -> table.getSerde().setUri(location));

    // update the input format if "hive.input-format" was set
    newProperties.entrySet().stream()
        .filter(entry -> SparkTables.HIVE_INPUT_FORMAT.equals(entry.getKey()))
        .map(Map.Entry::getValue)
        .forEach(inputFormat -> table.getSerde().setInputFormat(inputFormat));

    // update the output format if "hive.output-format" was set
    newProperties.entrySet().stream()
        .filter(entry -> SparkTables.HIVE_OUTPUT_FORMAT.equals(entry.getKey()))
        .map(Map.Entry::getValue)
        .forEach(outputFormat -> table.getSerde().setOutputFormat(outputFormat));

    // update the serde if "hive.serde" was set
    newProperties.entrySet().stream()
        .filter(entry -> SparkTables.HIVE_SERDE.equals(entry.getKey()))
        .map(Map.Entry::getValue)
        .forEach(serde -> table.getSerde().setSerializationLib(serde));

    table.setMetadata(tableProperties.build());

    table.getSerde().setSerdeInfoParameters(
        SparkTables.updateOptions(options(), table.getSerde().getUri(), newProperties, removedProperties));

    table.setFields(BatchUtil.toMetacatSchema(struct, partitioning()));

    this.table = client.getApi().updateTable(catalog, database, name, table);

    // invalidate lazy fields that may have changed
    this.lazySchema = null;
    this.lazyTransforms = null;
    this.lazyProperties = null;
    this.lazySpec = null;
    this.lazyPartitionSchema = null;
  }

  public String location() {
    return table.getSerde().getUri();
  }

  public Schema icebergSchema() {
    if (lazyIcebergSchema == null) {
      this.lazyIcebergSchema = SparkSchemaUtil.convert(schema());
    }
    return lazyIcebergSchema;
  }

  public PartitionSpec spec() {
    if (lazySpec == null) {
      this.lazySpec = Spark3Util.toPartitionSpec(icebergSchema(), partitioning());
    }
    return lazySpec;
  }

  public StructType partitionSchema() {
    if (lazyPartitionSchema == null) {
      this.lazyPartitionSchema = (StructType) SparkSchemaUtil.convert(spec().partitionType());
    }
    return lazyPartitionSchema;
  }

  public TableDto info() {
    return table;
  }

  public List<PartitionDto> partitions(Expression filter) {
    if (partitioning().length == 0) {
      PartitionDto rootPartition = new PartitionDto();
      rootPartition.setDataMetadata(table.getDataMetadata());
      rootPartition.setSerde(table.getSerde());
      return ImmutableList.of(rootPartition);
    }

    return client.getPartitionApi().getPartitions(
        catalog, database, name,
        BatchUtil.sql(filter),
        null /* sort by */, null /* sort order */,
        null /* start offset */, null /* limit */,
        true /* include metadata */);
  }

  public String provider() {
    return SparkTables.provider(table.getMetadata(), table.getSerde().getInputFormat());
  }

  public Map<String, String> options() {
    return table.getSerde().getSerdeInfoParameters();
  }

  public FileFormat format() {
    return SparkTables.format(provider());
  }

  @Override
  public Set<TableCapability> capabilities() {
    if (partitioning().length == 0) {
      return UNPARTITIONED_CAPABILITIES;
    } else {
      return CAPABILITIES;
    }
  }

  @Override
  public String toString() {
    return name();
  }

  private static StructType convertSchema(SparkSession spark, TableDto table) {
    ParserInterface parser = spark.sessionState().sqlParser();

    List<StructField> newFields = Lists.newArrayList();
    for (FieldDto fieldDto : table.getFields()) {
      DataType type;
      try {
        type = parser.parseDataType(fieldDto.getType());
      } catch (ParseException e) {
        throw new RuntimeException("Invalid data type: " + fieldDto.getType());
      }

      StructField field = new StructField(fieldDto.getName(), type, true, Metadata.empty());
      newFields.add(Optional.ofNullable(fieldDto.getComment())
          .filter(str -> !str.isEmpty())
          .map(field::withComment)
          .orElse(field));
    }

    return new StructType(newFields.toArray(new StructField[0]));
  }

  private static List<String> partitionColumns(TableDto table) {
    List<String> partitionColumns = Lists.newArrayList();
    for (FieldDto fieldDto : table.getFields()) {
      if (fieldDto.isPartition_key()) {
        partitionColumns.add(fieldDto.getName());
      }
    }

    return partitionColumns;
  }
}
