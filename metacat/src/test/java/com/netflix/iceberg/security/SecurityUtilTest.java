package com.netflix.iceberg.security;

import com.netflix.bdp.security.authorization.Acl;
import com.netflix.bdp.security.authorization.AclJsonParser;
import com.netflix.bdp.security.authorization.AuthPolicy;
import com.netflix.bdp.security.authorization.Privilege;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Condition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;

import static com.netflix.iceberg.security.SecurityUtil.DEFAULT_SECURE_BUCKET;
import static com.netflix.iceberg.security.SecurityUtil.WAREHOUSE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class SecurityUtilTest {

  private Configuration conf;
  private AuthPolicy authPolicy = AuthPolicy.PERMISSIVE;

  @Before
  public void init() {
    conf = new Configuration();
  }

  @Test
  public void updateLocation() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "location_update_test";

    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://some/path",
        ImmutableMap.of("grantor", "testUser@netflix.com")
    );

    tableMetadata = SecurityUtil.updateLocation(conf, TableIdentifier.of(catalog, database, tableName), tableMetadata);

    String expected = String.format("s3://%s/%s/%s.db/%s/%s", DEFAULT_SECURE_BUCKET, WAREHOUSE_PREFIX, database, tableMetadata.uuid(), tableName);
    String actual = tableMetadata.location();

    Assert.assertEquals("Table path mismatch", expected, actual);
  }

  @Test
  public void initializeACL() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "acl_test";

    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://bucket/path",
        ImmutableMap.of("grantor", "testUser@netflix.com")
    );

    tableMetadata = SecurityUtil.initializeACL(conf, TableIdentifier.of(catalog, database, tableName), tableMetadata, authPolicy, false);

    Assert.assertTrue("ACLs not in table metadata",
        tableMetadata.properties().containsKey(IcebergAclStorage.ACL_PROPERTY_KEY));
  }

  @Test
  public void testGrants() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "grants";

    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://bucket/path",
        ImmutableMap.of(
            "grantor", "testUser@netflix.com",
            "grant.select.user", "jsmith@netflix.com",
            "grant.delete.roles", "sec@netflix.com, admin@netflix.com"
        ));

    Configuration conf2 = new Configuration();
    conf2.set("grant.insert.user", "test2@netflix.com");
    conf2.set("grant.select.user", "test2@netflix.com");
    tableMetadata = SecurityUtil.initializeACL(conf2, TableIdentifier.of(catalog, database, tableName), tableMetadata, authPolicy, false);

    Assert.assertTrue("ACLs not in table metadata",
        tableMetadata.properties().containsKey(IcebergAclStorage.ACL_PROPERTY_KEY));

    Set<Acl> acls = AclJsonParser.fromJson(tableMetadata.properties().get(IcebergAclStorage.ACL_PROPERTY_KEY));

    Assert.assertEquals(4 /* one grantor, three user, one for roles */, acls.size());

    assertEquals(2, acls.stream().filter(acl -> acl.privileges().contains(Privilege.SELECT))
        .findFirst().orElseThrow(() -> new AssertionError("SELECT acl not found")).principals().size());

    assertEquals(2, acls.stream().filter(acl -> acl.privileges().contains(Privilege.DELETE))
        .findFirst().orElseThrow(() -> new AssertionError("DELETE acl not found")).principals().size());

    assertEquals(1, acls.stream().filter(acl -> acl.privileges().contains(Privilege.INSERT))
        .findFirst().orElseThrow(() -> new AssertionError("INSERT acl not found")).principals().size());
  }

  @Test
  public void testMultipleGrantors() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "grants";

    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://bucket/path",
        ImmutableMap.of(
                "grantor.user", "testUser_1@netflix.com, testUser_2@netflix.com",
                "grantor.role", "testGroup_1@netflix.com, testGroup_2@netflix.com",
                "grant.select.user", "jsmith@netflix.com"
        ));

    tableMetadata = SecurityUtil.initializeACL(conf, TableIdentifier.of(catalog, database, tableName), tableMetadata, authPolicy, false);

    Set<Acl> acls = AclJsonParser.fromJson(tableMetadata.properties().get(IcebergAclStorage.ACL_PROPERTY_KEY));

    assertThat(tableMetadata.properties()).as("ACLs not in table metadata").containsKey(IcebergAclStorage.ACL_PROPERTY_KEY);
    assertThat(acls).hasSize(2);
    Condition<Acl> withFourPrincipal = new Condition<>(m -> m.principals().size() == 4, "has 4 principal");
    assertThat(acls.stream().filter(Acl::withGrant)).haveExactly(1, withFourPrincipal); // 2 user grantor & 2 group grantor
    assertThat(acls.stream().filter(acl -> acl.privileges().contains(Privilege.SELECT))).hasSize(1);
    assertThat(acls.stream().filter(acl -> acl.privileges().contains(Privilege.INSERT))).isEmpty();
    assertThat(acls.stream().filter(acl -> acl.privileges().contains(Privilege.DELETE))).isEmpty();
  }

  @Test
  public void testGrantor() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "grants";
    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://bucket/path",
        ImmutableMap.of(
            "grantor", "testUser@netflix.com"
        ));
    tableMetadata = SecurityUtil.initializeACL(conf, TableIdentifier.of(catalog, database, tableName), tableMetadata, authPolicy, false);
    Assert.assertTrue("ACLs in table metadata",
        tableMetadata.properties().containsKey(IcebergAclStorage.ACL_PROPERTY_KEY));

    Set<Acl> acls = AclJsonParser.fromJson(tableMetadata.properties().get(IcebergAclStorage.ACL_PROPERTY_KEY));
    Assert.assertEquals(1 /* one grantor */, acls.size());
    Acl acl = acls.iterator().next();
    assertEquals(true, acl.privileges().contains(Privilege.ALL));
    assertEquals(1, acl.principals().size());
    assertEquals(true, acl.principals().contains(NetflixPrincipal.user("testUser@netflix.com")));
  }

  @Test
  public void testGrantorConf() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "grants";
    Configuration conf2 = new Configuration();
    conf2.set("grantor.user", "test2@netflix.com");
    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://bucket/path",
        ImmutableMap.of(
            "other", "missing"
        ));
    tableMetadata = SecurityUtil.initializeACL(conf2, TableIdentifier.of(catalog, database, tableName), tableMetadata, authPolicy, false);
    Assert.assertTrue("ACLs in table metadata",
        tableMetadata.properties().containsKey(IcebergAclStorage.ACL_PROPERTY_KEY));

    Set<Acl> acls = AclJsonParser.fromJson(tableMetadata.properties().get(IcebergAclStorage.ACL_PROPERTY_KEY));
    Assert.assertEquals(1 /* one grantor */, acls.size());
    Acl acl = acls.iterator().next();
    assertEquals(true, acl.privileges().contains(Privilege.ALL));
    assertEquals(1, acl.principals().size());
    assertEquals(true, acl.principals().contains(NetflixPrincipal.user("test2@netflix.com")));
  }

  @Test
  public void testGrantorRole() {
    String catalog = "prodhive";
    String database = "secure";
    String tableName = "grants";
    TableMetadata tableMetadata = TableMetadata.newTableMetadata(
        new Schema(),
        PartitionSpec.unpartitioned(),
        "s3://bucket/path",
        ImmutableMap.of(
            "grantor.role", "role@netflix.com"
        ));
    tableMetadata = SecurityUtil.initializeACL(conf, TableIdentifier.of(catalog, database, tableName), tableMetadata, authPolicy, false);
    Assert.assertTrue("ACLs in table metadata",
        tableMetadata.properties().containsKey(IcebergAclStorage.ACL_PROPERTY_KEY));

    Set<Acl> acls = AclJsonParser.fromJson(tableMetadata.properties().get(IcebergAclStorage.ACL_PROPERTY_KEY));
    Assert.assertEquals(1 /* one grantor */, acls.size());
    Acl acl = acls.iterator().next();
    assertEquals(true, acl.privileges().contains(Privilege.ALL));
    assertEquals(1, acl.principals().size());
    assertEquals(true, acl.principals().contains(NetflixPrincipal.group("role@netflix.com")));
  }
}
