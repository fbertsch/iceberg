package com.netflix.iceberg.metacat;

import com.netflix.bdp.security.authorization.Acl;
import com.netflix.bdp.security.authorization.AclJsonParser;
import com.netflix.bdp.security.authorization.AuthPolicy;
import com.netflix.iceberg.metacat.properties.ExternalPropertiesHandler;
import com.netflix.iceberg.metacat.properties.JsonPropertiesHandler;
import com.netflix.iceberg.security.MixedFileIO;
import com.netflix.iceberg.security.S3AuthStrategy;
import com.netflix.iceberg.security.SecurityContext;
import com.netflix.iceberg.security.SecurityUtil;
import com.netflix.iceberg.security.SimpleStsRefresher;
import com.netflix.iceberg.security.TableAuthMetadata;
import com.netflix.iceberg.security.TableAuthMetadataParser;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.nflxe2etokens.validation.common.E2eTokenConstants;
import com.netflix.s3authsign.common.rest.RemoteSigningAccessDeniedException;
import com.netflix.s3authsign.common.rest.S3StsAccessDeniedException;
import com.netflix.s3authsts.common.rest.StsCredentials;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.ipc.IpcLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.netflix.bdp.security.authorization.AuthPolicy.STRICT;
import static com.netflix.iceberg.metacat.DefinitionMetadata.SECURE_FLAG;
import static com.netflix.iceberg.metacat.DefinitionMetadata.setParentChildRelationship;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.CONF_EXPOSE_INTERNAL_STATES;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.CONF_INCLUDE_STS_CREDS_PROPS;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_INHERIT_ACL;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_OWNER_USER_ID;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_PROP_AUTH_POLICY;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_PROP_METADATA_LOC;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_PROP_MIGRATED_DATA_LOCATION;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_PROP_PARENT_TABLE_NAME;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.INTERNAL_PROP_PARENT_TABLE_UUID;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.LOAD_AUTH_ONLY_METADATA;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.PARENT_CHILD_RELATION_INFO;
import static com.netflix.iceberg.metacat.MetacatUtil.OWNER;
import static com.netflix.iceberg.metacat.MetacatUtil.getUser;
import static com.netflix.iceberg.security.IcebergAclStorage.ACL_PROPERTY_KEY;
import static com.netflix.iceberg.security.SecurityUtil.INITIALIZE_ACL;
import static com.netflix.iceberg.security.SecurityUtil.SIGNER_DEFAULT_APP_NAME;
import static com.netflix.iceberg.security.SecurityUtil.getSignerHost;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.CommitStatus.FAILURE;
import static org.apache.iceberg.BaseMetastoreTableOperations.CommitStatus.SUCCESS;
import static org.apache.iceberg.TableProperties.CLEANUP_METADATA_ON_COMMIT_FAILURE;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;

class MetacatClientOps extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetacatClientOps.class);
  private static final String SPARK_PROVIDER = "spark.sql.sources.provider";
  private static final String SPARK_NETFLIX_SECURE_FILEIO_ENABLED = "spark.netflix.secure-fileio-enabled";
  private static final boolean SPARK_NETFLIX_SECURE_FILEIO_ENABLED_DEFAULT = true;
  private static final Predicate<Exception> RETRY_IF = exc ->
      !exc.getClass().getCanonicalName().contains("Unrecoverable") &&
      !(exc instanceof RemoteSigningAccessDeniedException) &&
      !(exc instanceof S3StsAccessDeniedException) &&
      !(exc instanceof NullPointerException);

  private static final boolean METACAT_SUPPORTS_BRANCHING = false;

  private static final String NETFLIX_PREFIX = "netflix.";

  private final Configuration conf;
  private final MetacatApi metacatApi;
  private final TableIdentifier identifier;
  private final Client metacatClient;
  private final String catalog;
  private final String database;
  private final String table;
  private final String fullName;
  private boolean secure;
  private FileIO fileIO;
  private final SecurityContext securityContext;
  private boolean hasSpark;
  private S3AuthStrategy authStrategy;
  private int stsRefreshIfExpireInSecs;

  private final List<ExternalPropertiesHandler> externalPropertiesHandlers;
  private final List<JsonPropertiesHandler> jsonPropertyHandlers;

  MetacatClientOps(
          Configuration conf,
          Client client,
          TableIdentifier identifier,
          List<ExternalPropertiesHandler> externalPropertiesHandlers,
          List<JsonPropertiesHandler> jsonPropertyHandlers) {
    this.conf = conf;
    this.metacatApi = MetacatApi.builder()
        .withMetacatV1(client.getApi())
        .withIpcLogger(new IpcLogger(Spectator.globalRegistry(), LOG))
        .build();
    this.metacatClient = client;
    this.identifier = identifier;
    this.catalog = identifier.namespace().level(0);
    this.database = identifier.namespace().level(1);
    this.table = identifier.name();
    this.fullName = catalog + "." + database + "." + table;
    this.securityContext = new SecurityContext(identifier.toString());
    this.authStrategy = S3AuthStrategy.valueOf(conf.get("spark.netflix.authz-strategy", "STS"));
    this.stsRefreshIfExpireInSecs = conf.getInt("spark.netflix.authz.sts.refreshIfExpireInSecs", 300);
    this.externalPropertiesHandlers = ImmutableList.copyOf(externalPropertiesHandlers);
    this.jsonPropertyHandlers = ImmutableList.copyOf(jsonPropertyHandlers);

    try {
      Class.forName("org.apache.spark.sql.SparkSession");
      this.hasSpark = true;
    } catch(ClassNotFoundException ce){
      this.hasSpark = false;
    }

    refresh();
  }

  @Override
  protected String tableName() {
    return fullName;
  }

  private StsCredentials getStsCredentials() {
    this.io(); // init any required io which configures securityContext
    return new SimpleStsRefresher(this.securityContext).get();
  }

  @Override
  public synchronized void doRefresh() {
    String metadataLocation = null;
    try {
      TableDto tableInfo = warnLatency("load table %s.%s.%s from Metacat", catalog, database, table)
          .call(() -> MetacatUtil.getIcebergTable(metacatApi, catalog, database, table));
      Map<String, String> tableProperties = tableInfo.getMetadata();
      String tableType = tableProperties.get(TABLE_TYPE_PROP);
      this.secure = DefinitionMetadata.isSecure(tableInfo.getDefinitionMetadata());

      NoSuchIcebergTableException.check(
          ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType),
          "Entity %s.%s.%s exists but not an Iceberg table, tableType: %s",
          catalog, database, table, tableType);

      metadataLocation = tableProperties.get(METADATA_LOCATION_PROP);
      final String metadataLocationLocal = metadataLocation;
      NoSuchIcebergTableException.check(metadataLocation != null,
          "Invalid table, missing metadata_location: %s.%s.%s", catalog, database, table);

      Map<String, String> reserved = DefinitionMetadata.reservedProperties(tableInfo.getDefinitionMetadata());

      Function<String, TableMetadata> addReservedProperties = (loc) -> {
        Map<String, String> finalProperties = new HashMap<>();

        TableMetadata tableMetadata = getTableMetadata(loc);
        finalProperties.putAll(tableMetadata.properties());
        finalProperties.putAll(reserved);

        boolean isSecureTable = DefinitionMetadata.isSecure(tableInfo.getDefinitionMetadata());
        if (isSecureTable &&
          conf.getBoolean(CONF_INCLUDE_STS_CREDS_PROPS, false)) {
            LOG.info("Fetching sts credentials to include in properties for table: " + tableInfo.getName());
            StsCredentials credentials = getStsCredentials();
            Map<String, String> stsTokenProperties =
                    ImmutableMap.<String, String>builder()
                            .put("s3.access-key-id", credentials.getAccessKeyId())
                            .put("s3.secret-access-key", credentials.getSecretKey())
                            .put("s3.session-token", credentials.getSessionToken())
                            .put("s3.region", "us-east-1")
                            .build();
            finalProperties.putAll(stsTokenProperties);
        }

        // Expose internal states as table properties
        if (conf.getBoolean(CONF_EXPOSE_INTERNAL_STATES, false)) {
          ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
          builder.put(INTERNAL_PROP_METADATA_LOC, metadataLocationLocal); //add metadata location

          // Add auth policy if exists
          String authPolicyStr =  DefinitionMetadata.getAuthPolicy(tableInfo.getDefinitionMetadata());
          if(authPolicyStr != null) {
            builder.put(INTERNAL_PROP_AUTH_POLICY, AuthPolicy.valueOf(authPolicyStr).toString());
          }

          // Add migrated data location if exists
          String migratedDataLoc = DefinitionMetadata.getMigratedDataLoc(tableInfo.getDefinitionMetadata());
          if(migratedDataLoc != null) {
            builder.put(INTERNAL_PROP_MIGRATED_DATA_LOCATION, migratedDataLoc);
          }

          // Expose parent table name and uuid of table clones
          String[] parentTableNameAndUuid = getParentTableInfo(tableInfo);
          String parentName = parentTableNameAndUuid[0];
          if(!Strings.isNullOrEmpty(parentName)) {
            builder.put(INTERNAL_PROP_PARENT_TABLE_NAME, parentName.replace('/', '.'));
          }

          String parentUuid = parentTableNameAndUuid[1];
          if(!Strings.isNullOrEmpty(parentUuid)) {
            builder.put(INTERNAL_PROP_PARENT_TABLE_UUID, parentUuid);
          }

          String ownerUserId = DefinitionMetadata.getOwnerUserId(tableInfo.getDefinitionMetadata());
          builder.put(INTERNAL_OWNER_USER_ID, ownerUserId);

          finalProperties.putAll(builder.build());
        }

        for (ExternalPropertiesHandler handler : externalPropertiesHandlers) {
          Map<String, String> externalProperties = handler.loadProperties(identifier);
          finalProperties.putAll(externalProperties);
        }
        for (JsonPropertiesHandler handler : jsonPropertyHandlers) {
          Map<String, String> jsonProperties = handler.fromJson(identifier, tableInfo.getDefinitionMetadata());
          finalProperties.putAll(jsonProperties);
        }

        if (conf.getBoolean(LOAD_AUTH_ONLY_METADATA, false)) {
          tableMetadata = tableMetadata.addAdditionalPropertiesToAuthOnlyMetadata(finalProperties);
        } else {
          tableMetadata = tableMetadata.withAdditionalProperties(finalProperties);
        }

        // Table property takes precedence
        if (tableMetadata.properties().containsKey(CLEANUP_METADATA_ON_COMMIT_FAILURE) ||
            conf.get(CLEANUP_METADATA_ON_COMMIT_FAILURE) == null) {
          return tableMetadata;
        } else {
          final ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
              .putAll(tableMetadata.properties())
              .put(CLEANUP_METADATA_ON_COMMIT_FAILURE, conf.get(CLEANUP_METADATA_ON_COMMIT_FAILURE))
              .build();
          return tableMetadata.replaceProperties(properties);
        }
      };


      warnLatency("refresh metadata from %s", metadataLocationLocal)
          .run(() -> refreshFromMetadataLocation(metadataLocationLocal, RETRY_IF, 20, addReservedProperties));
    } catch (MetacatNotFoundException e) {
      // if metadata has been loaded for this table and is now gone, throw an exception
      // otherwise, assume the table doesn't exist yet.
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(format(
            "No such Metacat table: %s.%s.%s", catalog, database, table));
      }

      String metadataLocationLocal = metadataLocation;
      warnLatency("refresh metadata from %s after Metacat not found", metadataLocationLocal)
          .run(() -> refreshFromMetadataLocation(metadataLocationLocal, RETRY_IF, 20));
    }
  }

  private String[] getParentTableInfo(TableDto tableInfo) {
    JsonNode parentInfosNode = tableInfo.getDefinitionMetadata()
        .findPath(PARENT_CHILD_RELATION_INFO)
        .findPath("parentInfos");
    String name = parentInfosNode.findPath("name").asText();
    String uuid = parentInfosNode.findPath("uuid").asText();
    return new String[]{name, uuid};
  }

  private TableMetadata getTableMetadata(String loc) {
    if (this.conf.getBoolean(LOAD_AUTH_ONLY_METADATA, false)) {
      try {
        LOG.info("Loading auth only metadata");
        TableAuthMetadata tableAuthMetadata = TableAuthMetadataParser.get(io().newInputFile(loc));
        return new TableMetadata(
                loc,
                tableAuthMetadata.getLocation(),
                tableAuthMetadata.getTableUuid(),
                tableAuthMetadata.getProperties()
        );
      } catch (Exception e) {
        LOG.warn("Auth only metadata load failed. Reverting back to default metadata load", e);
        return TableMetadataParser.read(io(), loc);
      }
    } else {
      return TableMetadataParser.read(io(), loc);
    }
  }

  @Override
  public synchronized void doCommit(TableMetadata base, TableMetadata metadata) {
    metadata = updateOwner(metadata);
    // TODO - dgoya - delegate this to the handlers
    ObjectNode definitionMetadata = DefinitionMetadata.buildDefinitionMetadata(base, metadata);

    if (isCreateNewTable()) {
      // If the table properties contain a sort order but the sortOrder field is unsorted, we copy over the sort order.
      if (metadata.properties().containsKey("sort-order") && metadata.sortOrder().isUnsorted()) {
        metadata = metadata.replaceSortOrder(sortOrderFromString(metadata.schema(), metadata.properties().get("sort-order")));
      }

      boolean localSecure = DefinitionMetadata.isSecure(definitionMetadata) || shouldCreateSecureTable();
      if (localSecure && conf.getBoolean(SPARK_NETFLIX_SECURE_FILEIO_ENABLED, SPARK_NETFLIX_SECURE_FILEIO_ENABLED_DEFAULT)) {
        // If a table is being created, signal to the signing service
        securityContext.create(true);
        metadata = updateSecureLocation(metadata);
        securityContext.setCreationLocation(metadata.location());
        // Add auth_policy for new table
        AuthPolicy authPolicy = getAuthPolicy();
        DefinitionMetadata.setAuthPolicy(definitionMetadata, authPolicy);
        // Set instance `secure` so that io() is initialized as secure, which will be used for writing meta json
        this.secure = true;

        //Ensure the table is marked secure
        if (!DefinitionMetadata.isSecure(definitionMetadata)) {
          DefinitionMetadata.markSecure(definitionMetadata);
        }

        // For new secure table creation, always ignore existing acls
        metadata = metadata.removeProperties(a -> a.equals(ACL_PROPERTY_KEY));

        //Always ensure that an ACL entry exists for secure tables
        try {
          metadata = SecurityUtil.initializeACL(conf, identifier, metadata, authPolicy);
        } catch (SecurityException e) {
          if (!DefinitionMetadata.isAuthPolicyPermissive(definitionMetadata)) {
            throw e;
          }
        }
      } else if (SecurityUtil.isUseSecureLocation(conf)) {
        if (metadata.properties().getOrDefault(INITIALIZE_ACL, "false").equalsIgnoreCase("true")) {
          metadata = metadata.removeProperties(a -> a.equals(INITIALIZE_ACL));
          metadata = SecurityUtil.initializeACL(conf, identifier, metadata, STRICT);
        }
        // For presto to create table in secure location
        metadata = updateSecureLocation(metadata);
      }
    }

    validateBranchingEnabled(metadata);

    if (secure) {
      SecurityUtil.validateSecureBuckets(metadata.location(), metadata.properties());
      if (!isCreateNewTable() && metadata.properties().getOrDefault(INTERNAL_INHERIT_ACL, "false").equals("true")) {
        metadata = SecurityUtil.mergeExistingAndNewAcls(conf, identifier, metadata);
      }
    }
    metadata = metadata.removeProperties(x -> x.equals(INTERNAL_INHERIT_ACL));

    if(metadata.properties().containsKey(MetacatIcebergCatalog.CLONE_TABLE_SOURCE)) {
      String sourceName =  metadata.properties().get(MetacatIcebergCatalog.CLONE_TABLE_SOURCE);
      String[] sourceNames = sourceName.split("\\.");
      Preconditions.checkArgument(sourceNames.length == 3, "Invalid source table name: " + sourceName);

      TableDto sourceTableInfo = MetacatUtil.getIcebergTable(metacatApi, sourceNames[0], sourceNames[1], sourceNames[2]);
      Map<String, String> tableProperties = sourceTableInfo.getMetadata();
      String tableType = tableProperties.get(TABLE_TYPE_PROP);

      NoSuchIcebergTableException.check(
              ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType),
              "Entity %s.%s.%s exists but not an Iceberg table, tableType: %s",
              catalog, database, table, tableType);

      if (!DefinitionMetadata.isSecure(sourceTableInfo.getDefinitionMetadata())) {
        throw new BadRequestException("Clone table is only supported for secure tables. Source table: %s",
                sourceName);
      }

      String sourceMetadataLocation = tableProperties.get(METADATA_LOCATION_PROP);
      NoSuchIcebergTableException.check(sourceMetadataLocation != null,
              "Invalid table, missing metadata_location: %s.%s.%s", sourceNames[0], sourceNames[1], sourceNames[2]);
      Map<String, String> reserved = DefinitionMetadata.reservedProperties(sourceTableInfo.getDefinitionMetadata());
      MetacatClientOps sourceTableOps = new MetacatClientOps(conf, metacatClient, TableIdentifier.parse(sourceName), externalPropertiesHandlers, jsonPropertyHandlers);
      TableMetadata sourceMetadata = TableMetadataParser.read(sourceTableOps.io(), sourceMetadataLocation)
              .withAdditionalProperties(reserved);

      setParentChildRelationship(definitionMetadata, sourceName.replace('.', '/'), sourceMetadata.uuid(), metadata.uuid());

      metadata = mergeMetadataForCloneTable(metadata, sourceMetadata);
    }

    OperationContext operationContext = new OperationContext(getUser(metadata));
    for (ExternalPropertiesHandler handler : externalPropertiesHandlers) {
      Map<String, String> props = metadata.properties();
      String prefix = handler.prefix();
      Map<String, String> handlerProperties = props.entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      handler.saveProperties(
              identifier,
              handlerProperties,
              operationContext);
      // remove all handled properties from the set to be persisted
      // the handler will take care of populating them
      metadata = metadata.removeProperties(handlerProperties.keySet()::contains);
    }
    for (JsonPropertiesHandler handler : jsonPropertyHandlers) {
      Map<String, String> props = metadata.properties();
      String prefix = handler.prefix();
      Map<String, String> handlerProperties = props.entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      ObjectNode propertiesJson = handler.toJson(
              identifier,
              handlerProperties,
              operationContext
      );
      definitionMetadata = DefinitionMetadata.overwriteMerge(definitionMetadata, propertiesJson);
      // remove all handled properties from the set to be persisted
      // the handler will take care of populating them
      metadata = metadata.removeProperties(handlerProperties.keySet()::contains);
    }

    String newMetadataLocation = writeNewMetadata(
        metadata
            .removeProperties(DefinitionMetadata::isReservedProperty)
            .removeProperties(MetacatIcebergCatalog::isInternalProperty),
        currentVersion() + 1);

    CommitStatus commitStatus = FAILURE;
    try {
      StorageDto serde = new StorageDto();
      // set the Spark data source provider
      serde.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
      serde.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
      serde.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      serde.setUri(metadata.location());
      // set the table owner from the current user
      serde.setOwner(MetacatUtil.getUser(metadata));

      TableDto newTableInfo = new TableDto();
      newTableInfo.setName(QualifiedName.ofTable(catalog, database, table));
      newTableInfo.setSerde(serde);
      newTableInfo.setDataExternal(true);

      // forward any changed flink.watermark properties to definition metadata
      if (definitionMetadata != null) {
        newTableInfo.setDefinitionMetadata(definitionMetadata);
      }

      if (base != null) {
        newTableInfo.setMetadata(ImmutableMap.of(
            SPARK_PROVIDER, ICEBERG_TABLE_TYPE_VALUE,
            TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
            METADATA_LOCATION_PROP, newMetadataLocation,
            PREVIOUS_METADATA_LOCATION_PROP, base.metadataFileLocation()
        ));

        if(secure) {
          ensureNoLocationUpdate(base, metadata);
        }

        try {
          if (metadata != null &&
              base != null &&
              metadata.currentSnapshot() != null &&
              base.currentSnapshot() != null &&
              base.metadataFileLocation() != null) {
            LOG.info("Attempting: committing snapshot " + metadata.currentSnapshot().snapshotId()
                + " with expected snapshot = " + base.currentSnapshot().snapshotId()
                + " parent id = " + metadata.currentSnapshot().parentId()
                + " expected location = " + base.metadataFileLocation()
                + " trying to commit location = " + newMetadataLocation
            );
          }
          metacatApi.updateTable(catalog, database, table, newTableInfo);
        } catch (MetacatPreconditionFailedException e) {
          throw e;
        } catch (Throwable exception) {
            commitStatus = checkCommitStatus(newMetadataLocation, metadata, database, table);
            handleCommitFailure(exception, commitStatus);
        }
      } else {
        // if creating a migrated table, copy the TTL settings and other definition metadata
        if (isMigratedFromHive(metadata)) {
          if (table.endsWith("_iceberg")) {
            String backupTableName = table.substring(0, table.length() - 8) + "_hive";

            try {
              TableDto table = warnLatency("load table %s.%s.%s from Metacat", catalog, database, backupTableName)
                  .call(() -> MetacatUtil.getIcebergTable(metacatApi, catalog, database, backupTableName));
              // copy all of the definition metadata, and merge with new values
              ObjectNode dmFromBackupTable = table.getDefinitionMetadata();
              ObjectNode dmMerged = DefinitionMetadata.overwriteMerge(dmFromBackupTable, definitionMetadata);
              newTableInfo.setDefinitionMetadata(dmMerged);

            } catch (MetacatNotFoundException e) {
              LOG.warn("Cannot find backup table {}.{}.{}, not copying definition metadata",
                  catalog, database, backupTableName);
            }
          } else {
            LOG.warn("Expected temporary table name ending in '_iceberg': {}", table);
          }
        }

        newTableInfo.setMetadata(ImmutableMap.of(
            SPARK_PROVIDER, ICEBERG_TABLE_TYPE_VALUE,
            TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
            METADATA_LOCATION_PROP, newMetadataLocation
        ));
        try {
          metacatApi.createTable(catalog, database, table, newTableInfo);
        } catch (MetacatAlreadyExistsException e) {
          throw e;
        } catch (Throwable exception) {
          commitStatus = checkCommitStatus(newMetadataLocation, metadata, database, table);
          handleCommitFailure(exception, commitStatus);
        }
        if(secure && conf.getBoolean(SPARK_NETFLIX_SECURE_FILEIO_ENABLED, SPARK_NETFLIX_SECURE_FILEIO_ENABLED_DEFAULT)) {
          securityContext.create(false);
          securityContext.setCreationLocation(null);
          if (Closeable.class.isInstance(io())) {
            Closeable.class.cast(io()).close();
          }
          this.fileIO = null;
        }
      }

      commitStatus = SUCCESS;
    } catch (MetacatPreconditionFailedException e) {
      throw new CommitFailedException(e, "Failed to commit due to conflict");
    } catch (MetacatBadRequestException | MetacatUserMetadataException e) {
      throw new ValidationException(e, "Failed to commit: invalid request", e.getMessage());
    } catch (MetacatAlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists: %s.%s", database, table);
    } catch (MetacatException e) {
      throw new UncheckedIOException("Failed to commit", new IOException(e));
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (Throwable t) {
      throw new UncheckedIOException("Failed to commit", new IOException(t));
    } finally {
      if (commitStatus == FAILURE) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  private void validateBranchingEnabled(TableMetadata metadata){
    if (!METACAT_SUPPORTS_BRANCHING) {
      failIfContainsBranches(metadata);
    }
  }

  private void failIfContainsBranches(TableMetadata metadata) {
    for (Map.Entry<String, SnapshotRef> snapshotRef : metadata.refs().entrySet() ){
      if ( snapshotRef.getValue().isBranch() && !snapshotRef.getKey().equals("main") ){
        throw new UnsupportedOperationException("Branching is not supported at the moment");
      }
    }
  }

  private String createCloneTableACLs(TableMetadata metadata, String sourceTableACLs) {
    // Fix the UUID and DBName to match that of the clone Table
    String cloneDbName = database;
    String cloneTableUUID = metadata.uuid();
    Set<Acl> sourceACLs = AclJsonParser.fromJson(sourceTableACLs);
    SecurityUtil.replaceAclUUIDAndDB(sourceACLs, cloneDbName, cloneTableUUID);
    // merge in the ACLs from the clone table
    // TODO: Ensure principal here is part of source table ACL.
    if (metadata.properties().containsKey(ACL_PROPERTY_KEY)) {
      sourceACLs.addAll(AclJsonParser.fromJson(metadata.properties().get(ACL_PROPERTY_KEY)));
    }
    return AclJsonParser.toJson(sourceACLs);
  }

  /**
   * Merge clone and source table metadata.
   * @param metadata
   * @param src
   * @return
   */
  private TableMetadata mergeMetadataForCloneTable(TableMetadata metadata, TableMetadata src) {
    Map<String, String> srcProps = new HashMap<>(src.properties());

    // Remove this so that new metadata won't be written to old location
    srcProps.remove(WRITE_METADATA_LOCATION);
    srcProps.put(ACL_PROPERTY_KEY, createCloneTableACLs(metadata, srcProps.get(ACL_PROPERTY_KEY)));

    // Disable gc for cloned tables to avoid deleting files referenced by other tables with shared location
    srcProps.put(GC_ENABLED, "false");

    Map<String, String> finalProps = ImmutableMap.copyOf(srcProps);

    boolean includeSnapshots = Boolean.parseBoolean(metadata.properties()
            .getOrDefault(MetacatIcebergCatalog.CLONE_TABLE_WITH_SNAPSHOTS, "false"));
    if (includeSnapshots) {
      return TableMetadata.buildFrom(src)
              .withMetadataLocation(metadata.metadataFileLocation())
              .setLocation(metadata.location())
              .assignUUID(metadata.uuid())
              .setProperties(finalProps)
              .build();
    }
    return TableMetadata.buildFrom(src)
            .withMetadataLocation(metadata.metadataFileLocation())
            .setLocation(metadata.location())
            .assignUUID(metadata.uuid())
            .setProperties(finalProps)
            .removeSnapshots(src.snapshots())
            .build();
  }

  private static void ensureNoLocationUpdate(TableMetadata base, TableMetadata metadata) {
    Preconditions.checkArgument(Objects.equals(base.location(), metadata.location()),
            "Secure table does not allow location update");
  }

  private static boolean isMigratedFromHive(TableMetadata metadata) {
    return Boolean.parseBoolean(metadata.properties().getOrDefault("migrated-from-hive", "false"));
  }

  private AuthPolicy getAuthPolicy() {
    return SecurityUtil.isStrictDatabase(this.conf, this.identifier) ? AuthPolicy.STRICT :
            AuthPolicy.valueOf(this.conf.get("spark.netflix.authz-policy", AuthPolicy.PERMISSIVE.name()));
  }

  private TableMetadata updateSecureLocation(TableMetadata metadata) {
    metadata = SecurityUtil.updateLocation(conf, identifier, metadata);
    return metadata;
  }

  private TableMetadata updateOwner(TableMetadata metadata) {
    // Spark sets 'owner' to current user by default. Overriding it with 'netflix.owner'
    if (metadata.properties().containsKey(OWNER)) {
      return metadata.withAdditionalProperties(ImmutableMap.of(OWNER, MetacatUtil.getUser(metadata)));
    }
    return metadata;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      S3FileIOProperties properties = new S3FileIOProperties();
      try {
        getStagingDirectory(conf)
            .map(File::getAbsolutePath)
            .ifPresent(properties::setStagingDirectory);
      } catch (IOException e) {
        LOG.error("Failed to locate staging directory", e);
        throw new UncheckedIOException(e);
      }

      if (secure && conf.getBoolean(SPARK_NETFLIX_SECURE_FILEIO_ENABLED, SPARK_NETFLIX_SECURE_FILEIO_ENABLED_DEFAULT)) {
        S3ClientSupplier clientSupplier = createS3ClientSupplier(authStrategy);
        fileIO = new MixedFileIO(conf, clientSupplier, properties);
      } else if (conf.getBoolean("iceberg.s3fileio-enabled", false)) {
        // Role mapping is to support DAS use case and should be removed after roles are deprecated
        // FIXME: this does not support the bdp-s3fs bucket mapping currently.  It's unclear if role
        //        support will be needed moving forward.
        final String roleArn = conf.get("hive.s3.role.mapping."+database, conf.get("aws.iam.role.arn"));
        final int sessionDurationSecs = conf.getInt("aws.iam.role.session.duration.secs", 3600);
        SerializableSupplier<S3Client> clientSupplier = new S3ClientWithRoleSupplier(roleArn, sessionDurationSecs,
            S3UserAgentProvider.of(conf));
        fileIO = new S3FileIO(clientSupplier, properties);
      } else {
        fileIO = new HadoopFileIO(conf);
      }
    }

    return fileIO;
  }

  private S3ClientSupplier createS3ClientSupplier(S3AuthStrategy authStrategy) {
    final String signerAppName = conf.get("iceberg.s3.signer.app", SIGNER_DEFAULT_APP_NAME);
    securityContext.setSignerServiceHost(getSignerHost(conf));
    securityContext.setSignerRegion(conf.get("iceberg.s3.signer.region", Region.US_EAST_1.id()));
    securityContext.setSignerAppName(signerAppName);

    if (conf.get(E2eTokenConstants.E2ETOKEN_HEADER) != null) {
      securityContext.setE2eTokenSupplier(() -> {
        if (hasSpark) {
          return SparkSession.active().sparkContext().hadoopConfiguration().get(E2eTokenConstants.E2ETOKEN_HEADER);
        } else {
          return conf.get(E2eTokenConstants.E2ETOKEN_HEADER);
        }
      });
    }

    S3ClientSupplier s3ClientSupplier;
    switch (authStrategy) {
      case SIGN:
        s3ClientSupplier = new S3ClientWithSignerSupplier(securityContext);
        break;
      case STS:
        securityContext.setupStsCredentialsProvider(stsRefreshIfExpireInSecs);
        s3ClientSupplier = new S3ClientWithStsSupplier(securityContext);
        break;
      default:
        throw new UnsupportedOperationException("Invalid Authentication Strategy: " + authStrategy);

    }
    return s3ClientSupplier;
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    if (isCreateNewTable()) {
      boolean localSecure = uncommittedMetadata.propertyAsBoolean(SECURE_FLAG, false) ||
              shouldCreateSecureTable();
      if (localSecure && conf.getBoolean(SPARK_NETFLIX_SECURE_FILEIO_ENABLED, SPARK_NETFLIX_SECURE_FILEIO_ENABLED_DEFAULT)) {
        uncommittedMetadata = updateSecureLocation(uncommittedMetadata);

        //The purpose of this existence check is actually to trigger the token
        //creation by the external signer on the driver so the proper identity will
        //be propagated to the executors when files are created.
        securityContext.create(true);
        securityContext.setCreationLocation(uncommittedMetadata.location());
        this.secure = true;
        if (io().newInputFile(uncommittedMetadata.location()).exists()) {
          throw new AlreadyExistsException("Table already exists:" + identifier);
        }
      } else if (SecurityUtil.isUseSecureLocation(conf)) {
        // For presto to create table in secure location
        uncommittedMetadata = updateSecureLocation(uncommittedMetadata);
      }
    }

    TableMetadata tempMetadata = uncommittedMetadata;

    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return tempMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException("Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return MetacatClientOps.this.metadataFileLocation(tempMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(tempMetadata.location(), tempMetadata.properties());
      }

      @Override
      public FileIO io() {
        return MetacatClientOps.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return MetacatClientOps.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return MetacatClientOps.this.newSnapshotId();
      }
    };
  }

  private boolean shouldCreateSecureTable() {
    return SecurityUtil.isSecureDatabase(conf, identifier) || SecurityUtil.isNewTableAlwaysSecure(conf);
  }

  private boolean isCreateNewTable() {
    return currentVersion() < 0;
  }

  private static final String S3_STAGING_DIRECTORY = "bdp.s3.staging-directory";

  private static Optional<File> getStagingDirectory(Configuration conf) throws IOException {
    List<String> dirs = Arrays.asList(conf.getTrimmedStrings(S3_STAGING_DIRECTORY));
    Collections.shuffle(dirs);

    for(String dir : dirs) {
      File f = new File(dir);
      if (f.exists() && !f.isDirectory()) {
        throw new IOException("Configured staging path is not a directory: " + dir);
      }
      if (!f.exists()) {
          if (!f.mkdirs()) {
              LOG.warn("Staging directory {} was not created", dir);
          }
      }
      if (f.exists() && f.isDirectory() && f.canWrite()) {
        return Optional.of(f);
      }
    }

    return Optional.empty();
  }

  private WarnLatency warnLatency(String format, Object ...args) {
    return WarnLatency.builder()
        .withThreshold(MetacatUtil.latencyThresholdMs(conf))
        .withLogger(LOG)
        .withDescription(format, args)
        .build();
  }

  public static SortOrder sortOrderFromString(Schema schema, String sortOrderStr) {
    String[] fieldOrders = sortOrderStr.split(",(?![^()]*\\))");
    SortOrder.Builder builder = SortOrder.builderFor(schema);

    for (String fieldOrder : fieldOrders) {
      String transformStr;
      String[] parts = fieldOrder.trim().split("\\s+(?![^()]*\\))");
      UnboundTerm transform;
      if (fieldOrder.contains("(") && fieldOrder.contains(")")) {
        transformStr = parts[0];

        String functionName = transformStr.substring(0, transformStr.indexOf("(")).trim();
        String[] transformParts = transformStr.substring(transformStr.indexOf("(") + 1, transformStr.indexOf(")")).split(",");

        String fieldName = transformParts[0].trim();
        switch (functionName.toLowerCase()) {
          case "bucket":
            fieldName = transformParts[1].trim();
            int numBuckets = Integer.parseInt(transformParts[0].trim());
            transform = Expressions.bucket(fieldName, numBuckets);
            break;
          case "day":
            transform = Expressions.day(fieldName);
            break;
          case "hour":
            transform = Expressions.hour(fieldName);
            break;
          case "month":
            transform = Expressions.month(fieldName);
            break;
          case "identity":
            transform = Expressions.ref(fieldName);
            break;
          case "truncate":
            fieldName = transformParts[1].trim();
            int width = Integer.parseInt(transformParts[0].trim());
            transform = Expressions.truncate(fieldName, width);
            break;
          case "year":
            transform = Expressions.year(fieldName);
            break;
          default:
            throw new IllegalArgumentException("Unsupported transform function: " + functionName);
        }
      } else {
        transform = Expressions.ref(parts[0]);
      }

      if (parts[1].equalsIgnoreCase("ASC")) {
        if (parts[3].equalsIgnoreCase("FIRST")) {
          builder.asc(transform, NullOrder.NULLS_FIRST);
        } else {
          builder.asc(transform, NullOrder.NULLS_LAST);
        }
      } else if (parts[1].equalsIgnoreCase("DESC")) {
        if (parts[3].equalsIgnoreCase("FIRST")) {
          builder.desc(transform, NullOrder.NULLS_FIRST);
        } else {
          builder.desc(transform, NullOrder.NULLS_LAST);
        }
      }
    }

    return builder.build();
  }
}
