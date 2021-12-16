package com.netflix.iceberg.metacat;


import com.netflix.bdp.security.authorization.AuthPolicy;
import com.netflix.iceberg.security.SecurityContext;
import com.netflix.iceberg.security.SecurityUtil;
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
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.nflxe2etokens.validation.common.E2eTokenConstants;
import com.netflix.s3authsign.common.rest.RemoteSigningAccessDeniedException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static com.netflix.iceberg.metacat.DefinitionMetadata.SECURE_FLAG;
import static com.netflix.iceberg.security.SecurityUtil.SIGNER_DEFAULT_APP_NAME;
import static com.netflix.iceberg.security.SecurityUtil.SIGNER_DEFAULT_URL;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.CommitStatus.FAILURE;
import static org.apache.iceberg.BaseMetastoreTableOperations.CommitStatus.SUCCESS;
import static org.apache.iceberg.TableProperties.CLEANUP_METADATA_ON_COMMIT_FAILURE;

class MetacatClientOps extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetacatClientOps.class);
  private static final String SPARK_PROVIDER = "spark.sql.sources.provider";
  private static final Predicate<Exception> RETRY_IF = exc ->
      !exc.getClass().getCanonicalName().contains("Unrecoverable") &&
      !(exc instanceof RemoteSigningAccessDeniedException);

  private final Configuration conf;
  private final Client client;
  private final TableIdentifier identifier;
  private final String catalog;
  private final String database;
  private final String table;
  private final String fullName;
  private boolean secure;
  private FileIO fileIO;
  private final SecurityContext securityContext;
  private boolean hasSpark;

  MetacatClientOps(Configuration conf, Client client, TableIdentifier identifier) {
    this.conf = conf;
    this.client = client;
    this.identifier = identifier;
    this.catalog = identifier.namespace().level(0);
    this.database = identifier.namespace().level(1);
    this.table = identifier.name();
    this.fullName = catalog + "." + database + "." + table;
    this.securityContext = new SecurityContext(identifier.toString());

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

  @Override
  public synchronized void doRefresh() {
    String metadataLocation = null;
    try {
      TableDto tableInfo = client.getApi().getTable(catalog, database, table,
          true /* send table fields, partition keys */,
          true /* do not send user definition metadata (including ttl settings) */,
          false /* do not send user data metadata (?) */);

      Map<String, String> tableProperties = tableInfo.getMetadata();
      String tableType = tableProperties.get(TABLE_TYPE_PROP);
      this.secure = DefinitionMetadata.isSecure(tableInfo.getDefinitionMetadata());

      NoSuchIcebergTableException.check(
          ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType),
          "Entity %s.%s.%s exists but not an Iceberg table, tableType: %s",
          catalog, database, table, tableType);

      metadataLocation = tableProperties.get(METADATA_LOCATION_PROP);
      NoSuchIcebergTableException.check(metadataLocation != null,
          "Invalid table, missing metadata_location: %s.%s.%s", catalog, database, table);

      Map<String, String> reserved = DefinitionMetadata.reservedProperties(tableInfo.getDefinitionMetadata());
      Function<String, TableMetadata> addReservedProperties = (loc) -> {
        TableMetadata tableMetadata = TableMetadataParser.read(io(), loc).withReservedProperties(reserved);
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

      refreshFromMetadataLocation(metadataLocation, RETRY_IF, 20, addReservedProperties);
    } catch (MetacatNotFoundException e) {
      // if metadata has been loaded for this table and is now gone, throw an exception
      // otherwise, assume the table doesn't exist yet.
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(format(
            "No such Metacat table: %s.%s.%s", catalog, database, table));
      }

      refreshFromMetadataLocation(metadataLocation, RETRY_IF, 20);
    }
  }

  @Override
  public synchronized void doCommit(TableMetadata base, TableMetadata metadata) {
    ObjectNode definitionMetadata = DefinitionMetadata.buildDefinitionMetadata(base, metadata);

    this.secure = DefinitionMetadata.isSecure(definitionMetadata) || SecurityUtil.isSecure(conf, identifier);

    if (secure && conf.getBoolean("spark.netflix.secure-fileio-enabled", true)) {
      if(currentVersion() < 0) {
        // If a table is being created, signal to the signing service
        securityContext.create(true);
        metadata = SecurityUtil.updateLocation(conf, identifier, metadata);
        // Add auth_policy for new table
        String authPolicyStr = conf.get("spark.netflix.authz-policy", AuthPolicy.PERMISSIVE.name());
        DefinitionMetadata.setAuthPolicy(definitionMetadata, AuthPolicy.valueOf(authPolicyStr));
      }

      //Ensure the table is marked secure
      if (!DefinitionMetadata.isSecure(definitionMetadata)) {
        DefinitionMetadata.markSecure(definitionMetadata);
      }

      //Always ensure that an ACL entry exists for secure tables
      metadata = SecurityUtil.initializeACL(conf, identifier, metadata);
    }

    String newMetadataLocation = writeNewMetadata(
        metadata.removeReservedProperties(DefinitionMetadata::isReservedProperty),
        currentVersion() + 1);

    CommitStatus commitStatus = FAILURE;
    try {
      StorageDto serde = new StorageDto();
      // set the Spark data source provider
      serde.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
      serde.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
      serde.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      serde.setUri(metadata.location());

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
            PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation()
        ));

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
          client.getApi().updateTable(catalog, database, table, newTableInfo);
        } catch (MetacatPreconditionFailedException e) {
          throw e;
        } catch (Throwable exception) {
            commitStatus = checkCommitStatus(newMetadataLocation, metadata, database, table);
            handleCommitFailure(exception, commitStatus);
        }
      } else {
        // if creating a migrated table, copy the TTL settings and other definition metadata
        boolean isMigrated = Boolean.parseBoolean(
            metadata.properties().getOrDefault("migrated-from-hive", "false"));
        if (isMigrated) {
          if (table.endsWith("_iceberg")) {
            String backupTableName = table.substring(0, table.length() - 8) + "_hive";

            try {
              TableDto table = client.getApi().getTable(catalog, database, backupTableName,
                  true /* send table fields, partition keys */,
                  true /* send user definition metadata (including ttl settings) */,
                  false /* do not send user data metadata (?) */);

              // copy all of the definition metadata
              newTableInfo.setDefinitionMetadata(table.getDefinitionMetadata());

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

        // set the table owner from the current user
        newTableInfo.getSerde().setOwner(MetacatUtil.getUser());
        try {
          client.getApi().createTable(catalog, database, table, newTableInfo);
        } catch (MetacatAlreadyExistsException e) {
          throw e;
        } catch (Throwable exception) {
          commitStatus = checkCommitStatus(newMetadataLocation, metadata, database, table);
          handleCommitFailure(exception, commitStatus);
        }
        if(secure && conf.getBoolean("spark.netflix.secure-fileio-enabled", true)) {
          securityContext.create(false);
          ((S3FileIO) io()).close();
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

  @Override
  public FileIO io() {
    if (fileIO == null) {
      AwsProperties properties = new AwsProperties();
      try {
        getStagingDirectory(conf)
            .map(File::getAbsolutePath)
            .ifPresent(properties::setS3fileIoStagingDirectory);
      } catch (IOException e) {
        LOG.error("Failed to locate staging directory", e);
        throw new UncheckedIOException(e);
      }

      if (secure && conf.getBoolean("spark.netflix.secure-fileio-enabled", true)) {
        securityContext.setSignerServiceUrl(conf.get("iceberg.s3.signer.host", SIGNER_DEFAULT_URL));
        securityContext.setSignerRegion(conf.get("iceberg.s3.signer.region", Region.US_EAST_1.id()));

        final SecurityContext localContext = this.securityContext;

        if (conf.get(E2eTokenConstants.E2ETOKEN_HEADER) != null) {
          localContext.setE2eTokenSupplier(() -> {
            if (hasSpark) {
              return SparkSession.active().sparkContext().hadoopConfiguration().get(E2eTokenConstants.E2ETOKEN_HEADER);
            } else {
              return conf.get(E2eTokenConstants.E2ETOKEN_HEADER);
            }
          });
        }
        final String signerAppName = conf.get("iceberg.s3.signer.app", SIGNER_DEFAULT_APP_NAME);
        SerializableSupplier<S3Client> clientSupplier = new S3ClientWithSignerSupplier(signerAppName, localContext);
        fileIO = new S3FileIO(clientSupplier, properties);
      } else if (conf.getBoolean("iceberg.s3fileio-enabled", false)) {
        // Role mapping is to support DAS use case and should be removed after roles are deprecated
        // FIXME: this does not support the bdp-s3fs bucket mapping currently.  It's unclear if role
        //        support will be needed moving forward.
        final String roleArn = conf.get("hive.s3.role.mapping."+database, conf.get("aws.iam.role.arn"));
        final int sessionDurationSecs = conf.getInt("aws.iam.role.session.duration.secs", 3600);
        SerializableSupplier<S3Client> clientSupplier = new S3ClientWithRoleSupplier(roleArn, sessionDurationSecs);
        fileIO = new S3FileIO(clientSupplier, properties);
      } else {
        fileIO = new HadoopFileIO(conf);
      }
    }

    return fileIO;
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    MetacatClientOps.this.secure = uncommittedMetadata.propertyAsBoolean(SECURE_FLAG, false) ||
      SecurityUtil.isSecure(conf, identifier);
    
    if (secure && conf.getBoolean("spark.netflix.secure-fileio-enabled", true)) {
      uncommittedMetadata = SecurityUtil.updateLocation(conf, identifier, uncommittedMetadata);

      //The purpose of this existence check is actually to trigger the token
      //creation by the external signer on the driver so the proper identity will
      //be propagated to the executors when files are created.
      if (currentVersion() < 0) {
        securityContext.create(true);
        if (io().newInputFile(uncommittedMetadata.location()).exists()) {
          throw new AlreadyExistsException("Table already exists:" + identifier);
        }
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
}
