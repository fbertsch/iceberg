package com.netflix.iceberg.metacat;

import com.netflix.iceberg.CreateMADSnapshotListener;
import com.netflix.iceberg.KSGatewayListener;
import com.netflix.iceberg.metacat.properties.JanitorPropertiesHandler;
import com.netflix.iceberg.metacat.properties.NdcPropertiesHandler;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class MetacatIcebergCatalog extends BaseMetastoreCatalog implements SupportsNamespaces {
  public static final String MIGRATED_DATA_LOCATION = "migrated_data_location";
  public static final String CLONE_TABLE_SOURCE = "clone_table_source";
  public static final String CLONE_TABLE_WITH_SNAPSHOTS = "clone_table_source_snapshots";
  public static final String CONF_EXPOSE_INTERNAL_STATES = "netflix.iceberg.expose-internal-states-as-properties";
  public static final String CONF_INCLUDE_STS_CREDS_PROPS = "netflix.secure.include-sts-creds";
  public static final String LOAD_AUTH_ONLY_METADATA = "netflix.secure.load-auth-only-metadata";
  public static final String INTERNAL_PROP_PREFIX = "netflix._internal_.";
  public static final String INTERNAL_PROP_METADATA_LOC = INTERNAL_PROP_PREFIX + "metadata_location";
  public static final String INTERNAL_PROP_AUTH_POLICY = INTERNAL_PROP_PREFIX + "auth_policy";
  public static final String INTERNAL_PROP_MIGRATED_DATA_LOCATION = INTERNAL_PROP_PREFIX + MIGRATED_DATA_LOCATION;
  public static final String INTERNAL_OWNER_USER_ID = INTERNAL_PROP_PREFIX + "owner_user_id";
  public static final String PARENT_CHILD_RELATION_INFO = "parentChildRelationInfo";
  public static final String PARENT_TABLE_NAME = "parent_table_name";
  public static final String INTERNAL_PROP_PARENT_TABLE_NAME = INTERNAL_PROP_PREFIX + PARENT_TABLE_NAME;
  public static final String PARENT_TABLE_UUID = "parent_table_uuid";
  public static final String INTERNAL_PROP_PARENT_TABLE_UUID = INTERNAL_PROP_PREFIX + PARENT_TABLE_UUID;
  public static final String CHILD_TABLE_UUID = "child_table_uuid";
  public static final String INTERNAL_INHERIT_ACL = INTERNAL_PROP_PREFIX + "inherit_acl";


  private static volatile boolean initialized = false;

  // Database Metadata property engines usually set
  // when the DB has to be created in a specific location.
  private static final String DB_LOCATION = "location";

  private static void initialize(String appName, String appId, Configuration conf) {
    if (!MetacatIcebergCatalog.initialized) {
      synchronized (MetacatIcebergCatalog.class) {
        if (!MetacatIcebergCatalog.initialized) {
          MetacatIcebergCatalog.initialized = true;
          KSGatewayListener.initialize(appName, appId, conf);
          CreateMADSnapshotListener.initialize(conf);
        }
      }
    }
  }

  private final Configuration conf;
  private final String metacatHost;
  private final String appName;
  private final Client dbClient;

  public MetacatIcebergCatalog(Configuration conf, String appName) {
    this(conf, null, appName);
  }

  public MetacatIcebergCatalog(Configuration conf, String appId, String appName) {
    this.conf = conf;
    this.metacatHost = conf.get("netflix.metacat.host");
    this.appName = appName;
    this.dbClient = newClient();

    MetacatIcebergCatalog.initialize(appName, appId, conf);
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.hasNamespace() && tableIdentifier.namespace().levels().length == 2;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new MetacatClientOps(
            conf,
            newClient(),
            tableIdentifier,
            Lists.newArrayList(new NdcPropertiesHandler(conf)),
            Lists.newArrayList(new JanitorPropertiesHandler())
    );
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    return MetacatUtil.defaultTableLocation(conf, dbClient, catalog, database, tableIdentifier.name());
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.putAll(properties);

    if (properties.containsKey("provider") && !properties.containsKey(TableProperties.DEFAULT_FILE_FORMAT)) {
      propertiesBuilder.put(TableProperties.DEFAULT_FILE_FORMAT, properties.get("provider"));
    }

    if (!properties.containsKey(JanitorPropertiesHandler.SNAPSHOT_TTL_PROP)
        && conf.getBoolean(JanitorPropertiesHandler.SET_DEFAULT_SNAPSHOT_TTL, true)) {
      propertiesBuilder.put(
          JanitorPropertiesHandler.SNAPSHOT_TTL_PROP,
              JanitorPropertiesHandler.DEFAULT_SNAPSHOT_TTL_DAYS.toString());
    }

    return super.createTable(identifier, schema, spec, propertiesBuilder.build());
  }

  /**
   * Clone a table from a source table.
   * @param sourceTableIdentifier The source table.
   * @param cloneTableIdentifier The clone table.
   * @param properties Any additional properties that need to be included in the clone Table
   * @param includeSnapshots Whether snapshots from the source table should be included.                   
   * @return The cloned table
   */
  public Table cloneTable(TableIdentifier sourceTableIdentifier, TableIdentifier cloneTableIdentifier,
                          Map<String, String> properties, boolean includeSnapshots) {
    if (!isValidIdentifier(cloneTableIdentifier)) {
      throw new NoSuchTableException("Identifiers must be catalog.database.table: %s", cloneTableIdentifier);
    }

    Client metacatClient = newClient();
    if (!MetacatUtil.doesTableExist(metacatClient, sourceTableIdentifier)) {
      throw new NoSuchTableException("Table does not exist: %s", sourceTableIdentifier);
    }
    if (MetacatUtil.doesTableExist(metacatClient, cloneTableIdentifier)) {
      throw new AlreadyExistsException("Table already exists: %s", cloneTableIdentifier);
    }

    // Set up the properties that would go into the cloneTableIdentifier
    Table sourceTable = loadTable(sourceTableIdentifier);
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.putAll(sourceTable.properties());
    if(properties != null) {
      propertiesBuilder.putAll(properties);
    }
    propertiesBuilder.put(CLONE_TABLE_SOURCE, sourceTable.name());
    propertiesBuilder.put(CLONE_TABLE_WITH_SNAPSHOTS, Boolean.toString(includeSnapshots));
    propertiesBuilder.put("secure", "true");
    Map<String, String> newProperties = propertiesBuilder.build();
    System.out.println("new Properties" + newProperties);

    return createTable(cloneTableIdentifier, sourceTable.schema(), sourceTable.spec(), propertiesBuilder.build());
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    if (!isValidIdentifier(tableIdentifier)) {
      return false;
    }

    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    String tableName = tableIdentifier.name();

    return MetacatUtil.dropTable(newClient(), catalog, database, tableName);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Identifiers must be catalog.database.table: %s", from);
    }
    Preconditions.checkArgument(isValidIdentifier(to), "Identifiers must be catalog.database.table: %s", to);

    String fromCatalog = from.namespace().level(0);
    String toCatalog = to.namespace().level(0);
    Preconditions.checkArgument(fromCatalog.equals(toCatalog),
        "Cannot move table between catalogs: from=%s and to=%s", fromCatalog, toCatalog);

    String fromDatabase = from.namespace().level(1);
    String toDatabase = to.namespace().level(1);
    Preconditions.checkArgument(fromDatabase.equals(toDatabase),
        "Cannot move table between databases: from=%s and to=%s", fromDatabase, toDatabase);

    String fromTableName = from.name();
    String toTableName = to.name();
    if (!fromTableName.equals(toTableName)) {
      try {
        newClient().getApi().renameTable(fromCatalog, fromDatabase, fromTableName, toTableName);
      } catch (MetacatNotFoundException e) {
        throw new NoSuchTableException(e, "Table does not exist: %s", from);
      } catch (MetacatAlreadyExistsException e) {
        throw new AlreadyExistsException(e, "Table already exists: %s", to);
      }
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(namespace.levels().length == 2,
            "Catalog and database name required to list tables: %s", namespace);
    String catalogName = namespace.level(0);
    String dbName = namespace.level(1);
    return dbClient.getApi().getDatabase(catalogName, dbName,
            false /* Include user-metadata */,
            true /* Include table names */)
      .getTables().stream().map(t -> TableIdentifier.of(dbName, t)).collect(Collectors.toList());
  }

  @Override
  protected String fullTableName(String catalogName, TableIdentifier identifier) {
    // identifiers for this catalog include the catalog name already
    return identifier.toString();
  }

  @Override
  public String name() {
    return metacatHost;
  }

  private Client newClient() {
    return MetacatUtil.newClient(appName, metacatHost, conf);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Preconditions.checkArgument(namespace.levels().length == 2,
            "Invalid database name: %s", namespace);

    String catalogName = namespace.level(0);
    String dbName = namespace.level(1);
    ImmutableMap.Builder<String, String> metadataBuilder = new ImmutableMap.Builder<>();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      if (DB_LOCATION.equals(entry.getKey())) {
        // TODO: See if we can update metacat to support this.
        throw new UnsupportedOperationException("Metacat doesn't support setting database location");
      } else {
        metadataBuilder.put(entry.getKey(), entry.getValue());
      }
    }

    try {
      DatabaseCreateRequestDto createRequestDto = DatabaseCreateRequestDto.builder()
        .metadata(metadataBuilder.build())
        .build();
      dbClient.getApi().createDatabase(catalogName, dbName, createRequestDto);
    } catch (MetacatAlreadyExistsException e) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    switch(namespace.levels().length) {
      case 1:
        // List all the databases.
        String catalogName = namespace.level(0);
        return dbClient.getApi().getCatalog(namespace.level(0)).getDatabases()
          .stream().map(db -> Namespace.of(catalogName, db)).collect(Collectors.toList());
      default:
        throw new NoSuchNamespaceException("Only database level namespaces are supported: %s", namespace);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    Preconditions.checkArgument(namespace.levels().length == 2,
            "Invalid database name: %s", namespace);
    try {
      String catalogName = namespace.level(0);
      String dbName = namespace.level(1);
      DatabaseDto databaseDto = dbClient.getApi().getDatabase(catalogName, dbName,
        true /* Include user-metadata */,
        false /* Include table names */);

        return (databaseDto.getMetadata() == null)
                ? ImmutableMap.of()
                : ImmutableMap.copyOf(databaseDto.getMetadata());
    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exists: %s", namespace.level(1));
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    Preconditions.checkArgument(namespace.levels().length == 2,
            "Invalid database name: %s", namespace);

    try {
      dbClient.getApi().deleteDatabase(namespace.level(0), namespace.level(1));
      return true;
    } catch (MetacatNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    Preconditions.checkArgument(namespace.levels().length == 2,
            "Invalid database name: %s", namespace);

    HashMap<String, String> metadata = new HashMap<>(loadNamespaceMetadata(namespace));
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (DB_LOCATION.equals(entry.getKey())) {
        // TODO: See if we can update metacat to support this.
        throw new UnsupportedOperationException("Metacat doesn't support setting database location");
      } else {
        metadata.put(entry.getKey(), entry.getValue());
      }
    }

    try {
      DatabaseCreateRequestDto createRequestDto = DatabaseCreateRequestDto.builder()
        .metadata(ImmutableMap.copyOf(metadata))
        .build();
      dbClient.getApi().updateDatabase(namespace.level(0), namespace.level(1), createRequestDto);
      return true;
    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exists: %s", namespace.level(1));
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    Preconditions.checkArgument(namespace.levels().length == 2,
            "Invalid database name: %s", namespace);

    HashMap<String, String> metadata = new HashMap<>(loadNamespaceMetadata(namespace));
    Optional.ofNullable(properties).orElse(Collections.emptySet()).stream()
      .forEach(p -> metadata.remove(p));

    try {
      DatabaseCreateRequestDto createRequestDto = DatabaseCreateRequestDto.builder()
        .metadata(ImmutableMap.copyOf(metadata))
        .build();
      dbClient.getApi().updateDatabase(namespace.level(0), namespace.level(1), createRequestDto);
      return true;
    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exists: %s", namespace.level(1));
    }
  }

  public static boolean isInternalProperty(String s) {
    return s.startsWith(INTERNAL_PROP_PREFIX);
  }
}
