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

import com.netflix.iceberg.batch.MetacatBatchCatalog;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.BaseCatalog;
import org.apache.iceberg.spark.RollbackStagedTable;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class NetflixSparkCatalog extends BaseCatalog implements ViewCatalog {
  // properties that are already consumed by createTable.
  private static final Set<String> CONSUMED_PROPERTIES = Sets.newHashSet("provider", "hive.stored-as");
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};

  private String name = null;
  private MetacatSparkCatalog icebergCatalog = null;
  private MetacatBatchCatalog batchCatalog = null;
  private boolean createParquetAsIceberg = false;
  private boolean createAvroAsIceberg = false;
  private boolean createOrcAsIceberg = false;

  private ViewCatalog viewCatalog = null;

  @Override
  public Catalog icebergCatalog() {
    return icebergCatalog.icebergCatalog();
  }

  @Override
  public String[] defaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  @Override
  public String[][] listNamespaces() {
    return batchCatalog.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return batchCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    return batchCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    batchCatalog.createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    batchCatalog.alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    return batchCatalog.dropNamespace(namespace, cascade);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return batchCatalog.listTables(namespace);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    try {
      return icebergCatalog.tableExists(ident) || batchCatalog.tableExists(ident);
    } catch (MetacatBadRequestException e) {
      // Metacat throws this when the table is corrupted. assume it exists so it can be deleted
      return true;
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return batchCatalog.loadTable(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident, version);
    } catch (NoSuchTableException e) {
      return batchCatalog.loadTable(ident, version);
    }
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    try {
      return icebergCatalog.loadTable(ident, timestamp);
    } catch (NoSuchTableException e) {
      return batchCatalog.loadTable(ident, timestamp);
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String format = getFormat(properties);
    if (useIceberg(format)) {
      return icebergCatalog.createTable(ident, schema, partitions, createIcebergTableProperties(properties, format));
    } else {
      return batchCatalog.createTable(ident, schema, partitions, properties);
    }
  }

  @Override
  public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] partitions,
                                 Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String format = getFormat(properties);
    if (useIceberg(format)) {
      return icebergCatalog.stageCreate(ident, schema, partitions, createIcebergTableProperties(properties, format));
    } else {
      Table table = batchCatalog.createTable(ident, schema, partitions, properties);
      return new RollbackStagedTable(batchCatalog, ident, table);
    }
  }

  @Override
  public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] partitions,
                                  Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    String format = getFormat(properties);
    if (useIceberg(format)) {
      // return icebergCatalog.stageReplace(ident, schema, partitions, createIcebergTableProperties(properties, format));
      Map<String, String> icebergProperties = createIcebergTableProperties(properties, format);
      try {
        return icebergCatalog.stageReplace(ident, schema, partitions, icebergProperties);
      } catch (NoSuchIcebergTableException e) {
        // the table exists, but was a batch table. drop it and try again
        if (!batchCatalog.dropTable(ident)) {
          throw new NoSuchTableException(ident);
        }
        // use stageCreateOrReplace because the table was manually dropped
        return icebergCatalog.stageCreateOrReplace(ident, schema, partitions, icebergProperties);
      }

    } else {
      // attempt to drop the table and fail if it doesn't exist
      if (!batchCatalog.dropTable(ident)) {
        throw new NoSuchTableException(ident);
      }

      try {
        // create the table with the batch catalog, then wrap it in a staged table that will delete to roll back
        Table table = batchCatalog.createTable(ident, schema, partitions, properties);
        return new RollbackStagedTable(batchCatalog, ident, table);

      } catch (TableAlreadyExistsException e) {
        // the table was deleted, but now already exists again. retry the replace.
        return stageReplace(ident, schema, partitions, properties);
      }
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] partitions,
                                          Map<String, String> properties) throws NoSuchNamespaceException {
    String format = getFormat(properties);
    if (useIceberg(format)) {
      Map<String, String> icebergProperties = createIcebergTableProperties(properties, format);
      try {
        return icebergCatalog.stageCreateOrReplace(ident, schema, partitions, icebergProperties);
      } catch (NoSuchIcebergTableException e) {
        // the table exists, but was a batch table. drop it and try again
        batchCatalog.dropTable(ident);
        return icebergCatalog.stageCreateOrReplace(ident, schema, partitions, icebergProperties);
      }

    } else {
      // drop the table if it exists
      batchCatalog.dropTable(ident);

      try {
        // create the table with the session catalog, then wrap it in a staged table that will delete to roll back
        Table table = batchCatalog.createTable(ident, schema, partitions, properties);
        return new RollbackStagedTable(batchCatalog, ident, table);

      } catch (TableAlreadyExistsException e) {
        // the table was deleted, but now already exists again. retry the replace.
        return stageCreateOrReplace(ident, schema, partitions, properties);
      }
    }
  }

  private String getFormat(Map<String, String> properties) {
    String provider = properties.get("provider");
    String storedAs = properties.get("hive.stored-as");

    if (provider == null || "hive".equalsIgnoreCase(provider)) {
      // the provider was missing or defaulted, so check the Hive config properties
      if (storedAs != null) {
        return storedAs;
      } else if (properties.containsKey("hive.input-format")) {
        return "input-format"; // could be any non-iceberg string
      } else {
        return "iceberg";
      }
    }

    // provider was set and is not a default value
    Preconditions.checkArgument(
        storedAs == null || provider.equalsIgnoreCase(storedAs),
        "Cannot use both STORED AS %s and USING %s", storedAs, provider);

    return provider;
  }

  private Map<String, String> createIcebergTableProperties(Map<String, String> properties, String format) {
    ImmutableMap.Builder<String, String> newProperties = ImmutableMap.builder();
    properties.entrySet().stream()
        .filter(entry -> !CONSUMED_PROPERTIES.contains(entry.getKey().toLowerCase(Locale.ROOT)))
        .forEach(newProperties::put);
    newProperties.put("provider", "iceberg");

    if (format != null && !format.equalsIgnoreCase("iceberg")) {
      newProperties.put(TableProperties.DEFAULT_FILE_FORMAT, format);
    }

    if (!properties.containsKey(DefinitionMetadata.SNAPSHOT_TTL_PROP)) {
      newProperties.put(
              DefinitionMetadata.SNAPSHOT_TTL_PROP,
              DefinitionMetadata.DEFAULT_SNAPSHOT_TTL_DAYS.toString());
    }
    
    return newProperties.build();
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    if (icebergCatalog.tableExists(ident)) {
      return icebergCatalog.alterTable(ident, changes);
    } else {
      return batchCatalog.alterTable(ident, changes);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      if (icebergCatalog.tableExists(ident)) {
        return icebergCatalog.dropTable(ident);
      } else {
        return batchCatalog.dropTable(ident);
      }
    } catch (MetacatBadRequestException e) {
      return batchCatalog.dropTable(ident);
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    if (icebergCatalog.tableExists(from)) {
      icebergCatalog.renameTable(from, to);
    } else {
      batchCatalog.renameTable(from, to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    icebergCatalog.invalidateTable(ident);
    batchCatalog.invalidateTable(ident);
  }

  @Override
  public Identifier[] listViews(String... namespace) throws NoSuchNamespaceException {
    return viewCatalog.listViews(namespace);
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    return viewCatalog.loadView(ident);
  }

  @Override
  public boolean viewExists(Identifier ident) {
    try {
      return viewCatalog.viewExists(ident);
    } catch (MetacatBadRequestException e) {
      // Metacat throws this when the view is corrupted. assume it exists so it can be deleted
      return true;
    }
  }

  @Override
  public void createView(
      Identifier ident, String sql, StructType schema, String[] catalogAndNamespace, Map<String, String> properties)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    viewCatalog.createView(ident, sql, schema, catalogAndNamespace, properties);
  }

  @Override
  public void replaceView(
      Identifier ident, String sql, StructType schema, String[] catalogAndNamespace, Map<String, String> properties)
      throws NoSuchViewException, NoSuchNamespaceException {
    viewCatalog.replaceView(ident, sql, schema, catalogAndNamespace, properties);
  }

  @Override
  public void createOrReplaceView(
      Identifier ident, String sql, StructType schema, String[] catalogAndNamespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    viewCatalog.createOrReplaceView(ident, sql, schema, catalogAndNamespace, properties);
  }

  @Override
  public void alterView(Identifier ident, ViewChange... changes) throws NoSuchViewException {
    viewCatalog.alterView(ident, changes);
  }

  @Override
  public boolean dropView(Identifier ident) {
    return viewCatalog.dropView(ident);
  }

  @Override
  public void renameView(Identifier oldIdent, Identifier newIdent)
      throws NoSuchViewException, ViewAlreadyExistsException {
    viewCatalog.renameView(oldIdent, newIdent);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.createParquetAsIceberg = options.getBoolean("parquet-enabled", createParquetAsIceberg);
    this.createAvroAsIceberg = options.getBoolean("avro-enabled", createAvroAsIceberg);
    this.createOrcAsIceberg = options.getBoolean("orc-enabled", createOrcAsIceberg);

    this.icebergCatalog = new MetacatSparkCatalog();
    icebergCatalog.initialize(name, options);

    this.batchCatalog = new MetacatBatchCatalog();
    batchCatalog.initialize(name, options);

    viewCatalog = new CommonViewCatalog();
    viewCatalog.initialize(name, options);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return "NetflixCatalog(" + name + ")";
  }

  private boolean useIceberg(String format) {
    if (format == null || "iceberg".equalsIgnoreCase(format)) {
      return true;
    } else if (createParquetAsIceberg && "parquet".equalsIgnoreCase(format)) {
      return true;
    } else if (createAvroAsIceberg && "avro".equalsIgnoreCase(format)) {
      return true;
    } else if (createOrcAsIceberg && "orc".equalsIgnoreCase(format)) {
      return true;
    }

    return false;
  }
}
