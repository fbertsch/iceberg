package com.netflix.iceberg.security;

import com.netflix.bdp.security.authorization.Acl;
import com.netflix.bdp.security.authorization.Privilege;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal.PrincipalType;
import com.netflix.bdp.security.authorization.resource.Catalog;
import com.netflix.bdp.security.authorization.resource.Schema;
import com.netflix.bdp.security.authorization.resource.Table;
import com.netflix.metatron.ipc.MetatronKeyStores;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.netflix.bdp.security.authorization.AclJsonParser.toJson;
import static com.netflix.iceberg.security.IcebergAclStorage.ACL_PROPERTY_KEY;
import static com.netflix.metatron.ipc.MetatronCertificateAttribute.APP_ORIGIN_USER;
import static com.netflix.metatron.ipc.MetatronCertificateAttribute.USER_CERT_USER_USERNAME;
import static java.util.Collections.singleton;

public class SecurityUtil {
  public static final String SIGNER_DEFAULT_URL = "dgws3authsign.bdc.cluster.us-east-1.prod.cloud.netflix.net";
  public static final String SIGNER_DEFAULT_APP_NAME = "dgws3authsign.bdc";

  static final String SECURE_BUCKET = "netflix.warehouse.secure.bucket";
  static final String DEFAULT_SECURE_BUCKET = "nflx-secure-dataeng-prod-us-east-1";
  static final String WAREHOUSE_PREFIX = "iceberg/warehouse";
  static final String SECURE_DATABASES = "netflix.warehouse.secure.databases";
  static final String SECURE_DATABASES_DEFAULT = "secure";
  private static final ImmutableMap<String, PrincipalType> GRANTORS = ImmutableMap.of(
    "grantor.role", PrincipalType.GROUP,
    "grantor.user", PrincipalType.USER,
    "grantor", PrincipalType.USER
  );

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
    String bucket = conf.get(SECURE_BUCKET, DEFAULT_SECURE_BUCKET);
    String database = identifier.namespace().level(1);
    String uuid = metadata.uuid();
    String table = identifier.name();

    String location = String.format("s3://%s/%s/%s.db/%s/%s", bucket, WAREHOUSE_PREFIX, database, uuid, table);

    return metadata.updateLocation(location);
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

      return Stream.of(USER_CERT_USER_USERNAME, APP_ORIGIN_USER)
          .filter((e) -> certificate.getNonCriticalExtensionOIDs().contains(e.getOid()))
          .map((e) -> new String(certificate.getExtensionValue(e.getOid())).trim())
          .findFirst().orElseThrow(() -> new SecurityException("Failed to locate principal"));
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
  public static boolean isSecure(Configuration conf, TableIdentifier tableIdentifier) {
    String database = tableIdentifier.namespace().level(1);

    List<String> secureDatabases = Arrays.asList(conf.getStrings(SECURE_DATABASES, SECURE_DATABASES_DEFAULT));

    return secureDatabases.contains(database);
  }
}
