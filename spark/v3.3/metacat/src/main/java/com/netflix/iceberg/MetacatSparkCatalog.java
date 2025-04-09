package com.netflix.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netflix.iceberg.functions.ReadManifestDataFilePaths;
import com.netflix.iceberg.functions.SimpleTransform;
import com.netflix.iceberg.metacat.MetacatIcebergCatalog;
import com.netflix.iceberg.metacat.MetacatUtil;
import com.netflix.iceberg.metacat.TableRef;
import com.netflix.iceberg.metacat.TableType;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Array;

public class MetacatSparkCatalog extends SparkCatalog implements FunctionCatalog {
  private static final Identifier[] FUNCTIONS = Stream.of(
      "years", "months", "days", "date", "hours", "data_file_paths")
      .map(name -> Identifier.of(new String[0], name))
      .toArray(Identifier[]::new);
  private static final Set<String> SIMPLE_TRANSFORMS = Sets.newHashSet("identity",
      "year", "years", "month", "months", "day", "days", "date", "hour", "hours", "date_hour", "date_and_hour");

  // keep track of Iceberg catalogs by Spark session (identified by identityHashCode) and Metacat URI.
  // this ensures that table references in the same session are always the same in-memory instance.
  private static Cache<Pair<Integer, String>, Catalog> CATALOG_CACHE = Caffeine.newBuilder().build();

  private String catalogName = null;
  private SparkSession lazySpark = null;

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }
    return lazySpark;
  }

  @Override
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;

    SparkSession spark = lazySparkSession();
    Configuration conf = new Configuration(spark.sessionState().newHadoopConf());

    String metacatUri = MetacatUtil.syncMetacatUri(options, conf);

    boolean cacheEnabled = Boolean.parseBoolean(options.getOrDefault("cache-enabled", "true"));
    if (cacheEnabled) {
      Pair<Integer, String> cacheKey = Pair.of(System.identityHashCode(spark), metacatUri);
      return CATALOG_CACHE.get(cacheKey, key -> newCatalog(spark, conf));
    } else {
      return newCatalog(spark, conf);
    }
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    if (namespace.length == 0) {
      return FUNCTIONS;
    }
    return new Identifier[0];
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    try {
      return super.loadFunction(ident);
    } catch (NoSuchFunctionException ignored) {
    }
    return loadExtraFunction(ident);
  }

  private UnboundFunction loadExtraFunction(Identifier ident) throws NoSuchFunctionException {
    String[] namespace = ident.namespace();
    if (namespace.length == 0 || isSystemNamespace(namespace)) {
      String lowerCaseName = ident.name().toLowerCase(Locale.ROOT);
      if (SIMPLE_TRANSFORMS.contains(lowerCaseName)) {
        return new SimpleTransform(ident);
      } else if ("data_file_paths".equals(lowerCaseName)) {
        return new ReadManifestDataFilePaths(ident);
      }
    }

    throw new NoSuchFunctionException(ident);
  }

  @Override
  protected TableIdentifier buildIdentifier(Identifier ident) {
    int namespaceLength = ident.namespace().length;
    String[] newNamespace = new String[namespaceLength + 1];
    Array.copy(ident.namespace(), 0, newNamespace, 1, namespaceLength);
    newNamespace[0] = catalogName;
    return TableIdentifier.of(Namespace.of(newNamespace), ident.name());
  }

  /**
   * The parent class will use the Iceberg catalog returned by
   * {@link #buildIcebergCatalog(String, CaseInsensitiveStringMap)}, but does not handle table names that were used in
   * Spark 2.3.2 to work around not having multi-part names. For example, db.table__snapshots or db.table$history@1234.
   * <p>
   * This overrides the parent to translate these special table names into multi-part identifiers, like
   * db.table.snapshots. This also re-creates
   * @return
   */
  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    TableRef ref;
    try {
      return super.loadTable(ident);
    } catch (NoSuchTableException e) {
      // try to parse the identifier as a metadata or snapshot table
      ref = TableRef.parse(ident.name());
      if (ident.name().equals(ref.table())) {
        // ref is not different, so throw the original NoSuchTableException
        throw e;
      }
    }

    Identifier sourceTableIdent = Identifier.of(ident.namespace(), ref.table());
    switch (ref.type()) {
      case DATA:
        return super.loadTable(snapshotIdentifier(sourceTableIdent, ref.at()));
      case FILES:
      case ENTRIES:
      case MANIFESTS:
      case PARTITIONS:
        return super.loadTable(snapshotIdentifier(metadataIdentifier(sourceTableIdent, ref.type()), ref.at()));
      case HISTORY:
      case SNAPSHOTS:
      case ALL_DATA_FILES:
      case ALL_MANIFESTS:
      case ALL_ENTRIES:
        return super.loadTable(metadataIdentifier(sourceTableIdent, ref.type()));
      default:
        throw new IllegalArgumentException("Unknown table type: " + ref.type());
    }
  }

  private Identifier metadataIdentifier(Identifier ident, TableType metadataTable) {
    return extendedIdentifier(ident, metadataTable.name().toLowerCase(Locale.ROOT));
  }

  private Identifier snapshotIdentifier(Identifier ident, Long snapshotId) {
    if (snapshotId == null) {
      return ident;
    } else {
      return extendedIdentifier(ident, "snapshot_id_" + snapshotId);
    }
  }

  private Identifier extendedIdentifier(Identifier ident, String extendedName) {
    int namespaceLength = ident.namespace().length;
    String[] newNamespace = Arrays.copyOf(ident.namespace(), namespaceLength + 1);
    newNamespace[namespaceLength] = ident.name();
    return Identifier.of(newNamespace, extendedName);
  }

  public String toString() {
    return "MetacatSparkCatalog(name=" + name() + ", catalog=" + catalogName + ")";
  }

  /**
   * Create a new MetacatIcebergCatalog for a Spark session and configuration.
   */
  private static Catalog newCatalog(SparkSession spark, Configuration conf) {
    return new MetacatIcebergCatalog(conf, spark.sparkContext().applicationId(), "spark");
  }

  private static boolean isSystemNamespace(String[] namespace) {
    return namespace.length == 1 && namespace[0].equalsIgnoreCase("system");
  }
}
