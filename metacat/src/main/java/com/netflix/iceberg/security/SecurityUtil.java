package com.netflix.iceberg.security;

import com.netflix.bdp.security.authorization.Acl;
import com.netflix.bdp.security.authorization.AclUtils;
import com.netflix.bdp.security.authorization.MembershipChecker;
import com.netflix.bdp.security.authorization.Privilege;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal.PrincipalType;
import com.netflix.bdp.security.authorization.resource.Catalog;
import com.netflix.bdp.security.authorization.resource.Schema;
import com.netflix.bdp.security.authorization.resource.Table;
import com.netflix.metatron.ipc.MetatronKeyStores;
import com.netflix.metatron.ipc.auth.MetatronAppAuthContext;
import com.netflix.metatron.ipc.auth.MetatronAuthContext;
import com.netflix.metatron.ipc.auth.MetatronAuthContextFactory;
import com.netflix.metatron.ipc.auth.MetatronUserAuthContext;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import static com.netflix.bdp.security.authorization.AclJsonParser.toJson;
import static com.netflix.iceberg.security.IcebergAclStorage.ACL_PROPERTY_KEY;
import static java.util.Collections.singleton;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;

public class SecurityUtil {
  public static final String SIGNER_DEFAULT_HOST = "dgws3authsign.bdc.cluster.us-east-1.prod.cloud.netflix.net";
  public static final String SIGNER_DEFAULT_APP_NAME = "dgws3authsign.bdc";

  public static final String SECURE_BUCKET = "netflix.warehouse.secure.bucket";
  public static final String DEFAULT_SECURE_BUCKET = "nflx-secure-dataeng-prod-us-east-1";
  public static final String SECURE_BUCKET_TEST = "netflix.warehouse.secure.bucket.test";
  public static final String DEFAULT_SECURE_BUCKET_TEST = "nflx-secure-dataeng-test-us-east-1";
  public static final String USE_SECURE_LOCATION = "netflix.warehouse.use-secure-location";
  static final String WAREHOUSE_PREFIX = "iceberg/warehouse";
  static final String SECURE_DATABASES = "netflix.warehouse.secure.databases";
  static final String STRICT_DATABASES = "netflix.warehouse.secure.strict.databases";
  static final String SECURE_DATABASES_DEFAULT = "secure";
  static final String NEW_TABLE_ALWAYS_SECURE = "netflix.warehouse.secure.always";
  static final String SAVE_ACL_AS_ID = "netflix.warehouse.secure.save-acl-as-id";
  private static final ImmutableMap<String, PrincipalType> GRANTORS = ImmutableMap.of(
    "grantor.role", PrincipalType.GROUP,
    "grantor.user", PrincipalType.USER,
    "grantor", PrincipalType.USER
  );
  public static final String SECURE_BUCKETS_PROPERTY = "secure-buckets";
  public static final Map<String, String> SECURE_BUCKETS_FOR_REGIONS = ImmutableMap.<String, String>builder()
      .put("nflx-secure-dataeng-prod-eu-west-1", "eu-west-1")
      .put("nflx-secure-dataeng-prod-us-east-1", "us-east-1")
      .put("nflx-secure-dataeng-prod-us-east-2", "us-east-2")
      .put("nflx-secure-dataeng-prod-us-west-2", "us-west-2")
      .put("nflx-secure-dataeng-test-eu-west-1", "eu-west-1")
      .put("nflx-secure-dataeng-test-us-east-1", "us-east-1")
      .put("nflx-secure-dataeng-test-us-east-2", "us-east-2")
      .put("nflx-secure-dataeng-test-us-west-2", "us-west-2")
      .build();

  /**
   * Update a secure table location based on it's uuid and name.
   *
   * Sets the required path prefix for a secure table including: bucket, prefix, and table identifier.
   * The first components in the path are required by the S3 Signer to ensure signed paths belong to owning table.
   *
   * @param identifier table id
   * @param metadata table metadata
   */
  public static TableMetadata updateLocation(Configuration conf, TableIdentifier identifier, TableMetadata metadata) {
    // Remove properties for custom data / metadata location
    if(metadata.properties().containsKey(WRITE_METADATA_LOCATION)) {
      Map<String, String> newProperties = Maps.newHashMap(metadata.properties());
      newProperties.remove(WRITE_METADATA_LOCATION);
      metadata = metadata.replaceProperties(newProperties);
    }

    String bucket = getSecureBucket(conf, identifier.namespace().level(0));
    String location = buildSecureTableLocation(bucket, identifier, metadata.uuid());
    return metadata.updateLocation(location);
  }

  public static String getSignerHost(Configuration conf) {
    return conf.get("iceberg.s3.signer.host", SIGNER_DEFAULT_HOST);
  }

  static MembershipChecker getMembershipChecker(Configuration conf) {
    if(conf.getBoolean(SAVE_ACL_AS_ID, true)) {
      return MembershipCheckerFactory.getOrCreate(getSignerHost(conf), true);
    }
    return MembershipCheckerFactory.NO_OP_CHECKER;
  }

  /**
   * Make sure location is included in secure-buckets property if exists, and all buckets from secure-buckets
   * property are in the pre-defined secure bucket set.
   */
  public static void validateSecureBuckets(String location, Map<String, String> properties) {
    String bucketsPropStr = properties.get(SECURE_BUCKETS_PROPERTY);
    if (bucketsPropStr == null) {
      return;
    }

    List<String> bucketsFromProps = Arrays.asList(bucketsPropStr.trim().split(","));
    String metadataBucket = SecurityUtil.extractS3Bucket(location);

    Preconditions.checkArgument(
        bucketsFromProps.contains(metadataBucket),
        String.format("Metadata bucket '%s' is not included in table property '%s': %s.",
            metadataBucket, SECURE_BUCKETS_PROPERTY, bucketsPropStr)
    );

    List<String> invalidBuckets = Lists.newArrayList(Iterables.filter(bucketsFromProps, b -> !SECURE_BUCKETS_FOR_REGIONS.containsKey(b)));
    Preconditions.checkArgument(
        invalidBuckets.isEmpty(),
        String.format("Found invalid buckets in table property '%s': %s", SECURE_BUCKETS_PROPERTY, invalidBuckets)
    );
  }

  private static String getSecureBucket(Configuration conf, String catalog) {
    if (catalog.toLowerCase(Locale.ROOT).contains("test")) {
      return conf.get(SECURE_BUCKET_TEST, DEFAULT_SECURE_BUCKET_TEST);
    }
    return conf.get(SECURE_BUCKET, DEFAULT_SECURE_BUCKET);
  }

  public static String buildSecureTableLocation(String bucket, TableIdentifier identifier, String uuid) {
    String database = identifier.namespace().level(1);
    String table = identifier.name();
    return String.format("s3://%s/%s/%s.db/%s/%s", bucket, WAREHOUSE_PREFIX, database, uuid, table);
  }

  /**
   * Set the ACL for a newly created table.  This includes the ability to configure ACLs for ETL jobs
   * by setting grants for users/roles.  The environment is used to identify the Metatron identity and
   * use that as the initial owner.
   *
   * grant.select.users
   * grant.insert.roles
   *
   * @param metadata table metadata
   * @param identifier identifier
   */
  public static TableMetadata initializeACL(Configuration conf, TableIdentifier identifier, TableMetadata metadata){
    if(metadata.properties().containsKey(ACL_PROPERTY_KEY)) {
      return metadata;
    }

    String catalog = identifier.namespace().level(0);
    String database = identifier.namespace().level(1);
    String table = identifier.name();

    Table resource = new Table(new Schema(new Catalog(catalog), database), table, metadata.uuid());
    final NetflixPrincipal grantor = findGrantor(conf, metadata);

    Map<Privilege, Set<NetflixPrincipal>> grants = parseGrants(conf, metadata.properties());

    Set<Acl> acls = grants.entrySet().stream()
        .map(e -> new Acl(e.getValue(), singleton(e.getKey()), singleton(resource), grantor, false))
        .collect(Collectors.toSet());

    // Explicitly add all for grantor
    acls.add(new Acl(singleton(grantor), singleton(Privilege.ALL), singleton(resource), grantor, true ));
    // Map all account names to ids before saving acls
    acls = AclUtils.mapNameToId(acls, getMembershipChecker(conf));

    // create updated properties and remove grants
    Map<String, String> newProperties = metadata.properties().entrySet().stream()
        .filter((e) -> !e.getKey().toUpperCase().startsWith("GRANT."))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    newProperties.put(ACL_PROPERTY_KEY, toJson(acls));

    return metadata.replaceProperties(newProperties);
  }

  private static NetflixPrincipal findGrantor(Configuration conf, TableMetadata metadata) {
    /* Check metadata properties first */
    NetflixPrincipal grantor = findGrantorInMap(metadata.properties());

    /* No grantor in metadata try config */
    if (grantor == null) {
      grantor = findGrantorInMap(
        GRANTORS.entrySet().stream()
          .filter((e) -> conf.get(e.getKey()) != null)
          .map((e) -> {
            return Collections.singletonMap(e.getKey(), conf.get(e.getKey())).entrySet();
          })
          .flatMap(Collection::stream)
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
      );
    }

    /* Still no grantor, try local metatron */
    if (grantor == null) {
      grantor = NetflixPrincipal.user(resolvePrincipal());
    }

    return grantor;
  }

  private static NetflixPrincipal findGrantorInMap(Map<String, String> m) {
    Set<NetflixPrincipal> grantors = GRANTORS.entrySet().stream()
        .filter((e) -> m.containsKey(e.getKey()) && m.get(e.getKey()) != null)
        .map((e) -> {
            return new NetflixPrincipal(m.get(e.getKey()), e.getValue());
        })
        .collect(Collectors.toSet());

    switch (grantors.size()) {
      case 0: return null;
      case 1: return grantors.iterator().next();
      default: throw new ValidationException("Only one grantor is permitted");
    }
  }

  /**
   * Parse grants of the form:
   *
   *   grant.<privilege>.[user(s)|role(s)] = principal[,principal...]
   */
  private static Map<Privilege, Set<NetflixPrincipal>> parseGrants(Configuration conf, Map<String, String> properties) {
    return Stream.concat(properties.entrySet().stream(), StreamSupport.stream(conf.spliterator(), false))
        .filter((e) -> e.getKey().toUpperCase().startsWith("GRANT."))
        .map((e) -> {
          String [] parts = e.getKey().split("\\.");

          if (parts.length != 3) {
            // Required key format: grant.<privilege>.[user(s)|role(s)]
            throw new ValidationException("Invalid grant specification: " + e.getKey());
          }

          Privilege privilege = Privilege.valueOf(parts[1].toUpperCase());
          String principalString = parts[2].toUpperCase();
          final PrincipalType principalType;

          // Allow for use of 'ROLE' since that's closer to SQL DCL conventions
          switch (principalString) {
            case "USER":
            case "USERS": principalType = PrincipalType.USER;
              break;
            case "GROUP":
            case "GROUPS":
            case "ROLE":
            case "ROLES": principalType = PrincipalType.GROUP;
              break;
            default: throw new RuntimeException("Unsupported principal type:" + principalString);
          }

          Set<NetflixPrincipal> principals = Arrays.stream(e.getValue().split(","))
              .map(String::trim)
              .map((p) -> new NetflixPrincipal(p, principalType))
              .collect(Collectors.toSet());

          return Collections.singletonMap(privilege, principals).entrySet();
        })
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(
          Map.Entry::getKey,
          Map.Entry::getValue,
            (s1, s2) -> {s1.addAll(s2); return s1;}
        ));
  }

  /**
   * Search the local environment for a user/client cert based on Metatron locations and try to
   * extract a user identity.
   *
   * @return principal identifier
   */
  private static String resolvePrincipal() {
    try {
      Path path = MetatronKeyStores.getMetatronHomeDirs().stream()
        .map(p -> ImmutableList.of(p.resolve("user.crt"), p.resolve("client.crt"))).flatMap(Collection::stream)
        .filter(Files::exists).filter(Files::isReadable).findFirst()
        .orElseThrow(() -> new SecurityException("Failed to load Metatron credentials."));

      X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(
          new FileInputStream(path.toFile()));

      MetatronAuthContext context = MetatronAuthContextFactory.fromCertificate(certificate);
      switch (context.getAuthContextType()) {
        case USER:
          return ((MetatronUserAuthContext)context).getUsername();
        case APP:
          if (((MetatronAppAuthContext)context).getOriginUser() != null) {
            return ((MetatronAppAuthContext)context).getOriginUser();
          }
      }
      throw new SecurityException("Failed to locate principal");
    } catch (CertificateException | FileNotFoundException e) {
      throw new SecurityException(e);
    }
  }

  /**
   * Return whether the table identifier should be secure or not.
   *
   * @param conf config
   * @param tableIdentifier identifier
   * @return secure flag
   */
  public static boolean isSecureDatabase(Configuration conf, TableIdentifier tableIdentifier) {
    String database = tableIdentifier.namespace().level(1);

    List<String> secureDatabases = Arrays.asList(conf.getStrings(SECURE_DATABASES, SECURE_DATABASES_DEFAULT));

    return secureDatabases.contains(database);
  }

  public static boolean isStrictDatabase(Configuration conf, TableIdentifier tableIdentifier) {
    String database = tableIdentifier.namespace().level(1);
    Collection<String> strictDataBases = conf.getStringCollection(STRICT_DATABASES);
    return strictDataBases.contains(database);
  }

  public static boolean isNewTableAlwaysSecure(Configuration conf) {
    return conf.getBoolean(NEW_TABLE_ALWAYS_SECURE, false);
  }

  public static boolean isUseSecureLocation(Configuration conf) {
    return conf.getBoolean(SecurityUtil.USE_SECURE_LOCATION, false);
  }

  public static String extractS3Bucket(String path) {
    if(path != null) {
      String[] parts = path.split("/");
      if(parts.length > 2) {
        return parts[2];
      }
    }
    return "";
  }

}
