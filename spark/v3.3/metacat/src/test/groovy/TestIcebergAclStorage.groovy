import com.netflix.bdp.security.authorization.Acl
import com.netflix.bdp.security.authorization.IcebergAclStorage
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal
import com.netflix.bdp.security.authorization.resource.Catalog
import com.netflix.bdp.security.authorization.resource.Column
import com.netflix.bdp.security.authorization.resource.Schema
import com.netflix.bdp.security.authorization.resource.Table
import org.apache.iceberg.TestTables
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.Types
import spock.lang.Specification

import static com.netflix.bdp.security.authorization.Privilege.ALL
import static com.netflix.bdp.security.authorization.Privilege.DELETE
import static com.netflix.bdp.security.authorization.Privilege.SELECT

class TestIcebergAclStorage extends Specification {
  static user = NetflixPrincipal.user("test@netflix.com")
  static group = NetflixPrincipal.group("group@netflix.com")
  static catalogName = "testcatalog"
  static schemaName = "testschema"
  static tableName = "testtable"
  static columnName = "testColumn"
  static catalog = new Catalog(catalogName)
  static schema = new Schema(catalog, schemaName)
  static table = new Table(schema, tableName)
  static column = new Column(table, columnName)
  static icebergSchema = new org.apache.iceberg.Schema(Types.NestedField.required(1, columnName, new Types.StringType()))
  def icebergCatalog = new TestTables.TestCatalog()
  def aclStorage = new IcebergAclStorage(icebergCatalog)

  def "Test ACL Storage for #resource # privilege and #principal"() {
    setup:
    icebergCatalog.createTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName), icebergSchema)

    when:
    def isEmpty = aclStorage.get(resource).isEmpty()

    then:
    isEmpty == true

    when:
    aclStorage.add([new Acl([principal].toSet(), [privilege].toSet(), [resource].toSet(), principal, false)].toSet())

    then:
    aclStorage.get(resource) == [new Acl([principal].toSet(), [privilege].toSet(), [resource].toSet(), principal, false)].toSet()

    when:
    def isRemoved = aclStorage.remove([new Acl([principal].toSet(), [privilege].toSet(), [resource].toSet(), principal, false)].toSet())

    then:
    isRemoved == true
    aclStorage.get(resource).isEmpty()

    cleanup:
    icebergCatalog.dropTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName))

    where:
    resource | privilege        | principal
    table    | DELETE | user
    column   | SELECT | group
  }

  def "Test ACL Storage add a privilege to existing acl"() {
    setup:
    icebergCatalog.createTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName), icebergSchema)

    when:
    aclStorage.get(table).isEmpty()

    then:
    true

    when:
    aclStorage.add([new Acl([user].toSet(), [SELECT].toSet(), [table].toSet(), user, false)].toSet())

    then:
    aclStorage.get(table) == [new Acl([user].toSet(), [SELECT].toSet(), [table].toSet(), user, false)].toSet()

    when:
    aclStorage.add([new Acl([user].toSet(), [DELETE].toSet(), [table].toSet(), user, false)].toSet())

    then:
    aclStorage.get(table) == [new Acl([user].toSet(), [SELECT].toSet(), [table].toSet(), user, false), new Acl([user].toSet(), [DELETE].toSet(), [table].toSet(), user, false)].toSet()

    cleanup:
    icebergCatalog.dropTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName))
  }

  def "Test ACL Storage remove a subset of privileges to existing acl"() {
    setup:
    icebergCatalog.createTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName), icebergSchema)

    when:
    aclStorage.add([new Acl([user].toSet(), [SELECT, DELETE].toSet(), [table].toSet(), user, false)].toSet())

    then:
    aclStorage.get(table) == [new Acl([user].toSet(), [SELECT, DELETE].toSet(), [table].toSet(), user, false)].toSet()

    when:
    def isRemoved = aclStorage.remove([new Acl([user].toSet(), [DELETE].toSet(), [table].toSet(), user, false)].toSet())

    then:
    isRemoved == true
    aclStorage.get(table) == [new Acl([user].toSet(), [SELECT].toSet(), [table].toSet(), user, false)].toSet()

    cleanup:
    icebergCatalog.dropTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName))
  }

  def "Test ACL Storage remove ALL privileges to existing acl"() {
    setup:
    icebergCatalog.createTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName), icebergSchema)

    when:
    aclStorage.add([new Acl([user].toSet(), [SELECT, DELETE].toSet(), [table].toSet(), user, false)].toSet())
    def isRemoved = aclStorage.remove([new Acl([user].toSet(), [ALL].toSet(), [table].toSet(), user, false)].toSet())

    then:
    isRemoved == true
    aclStorage.get(table) == [].toSet()

    cleanup:
    icebergCatalog.dropTable(TableIdentifier.of(Namespace.of(catalogName, schemaName), tableName))
  }

  // TODO add more tests with a single acl that has multiple principals,resources, privileges.
}