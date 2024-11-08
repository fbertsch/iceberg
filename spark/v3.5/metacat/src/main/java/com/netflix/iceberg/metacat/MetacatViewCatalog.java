package com.netflix.iceberg.metacat;

import com.netflix.bdp.view.BaseMetastoreViewOperations;
import com.netflix.bdp.view.BaseMetastoreViews;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.DatabaseDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class MetacatViewCatalog extends BaseMetastoreViews {

  private final Configuration conf;
  private final String metacatHost;
  private final String appName;
  private final Client dbClient;

  public MetacatViewCatalog(Configuration conf, String appName) {
    super(conf);
    this.conf = conf;
    this.metacatHost = conf.get("netflix.metacat.host");
    this.appName = appName;
    this.dbClient = newClient();
  }

  @Override
  public void rename(String oldIdentifier, String newIdentifier) {
    TableIdentifier from = toCatalogTableIdentifier(oldIdentifier);
    TableIdentifier to = toCatalogTableIdentifier(newIdentifier);

    validateTableIdentifier(from);
    validateTableIdentifier(to);

    String fromCatalog = from.namespace().level(0);
    String toCatalog = to.namespace().level(0);
    Preconditions.checkArgument(fromCatalog.equals(toCatalog),
        "Cannot move view between catalogs: from=%s and to=%s", fromCatalog, toCatalog);

    String fromDatabase = from.namespace().level(1);
    String toDatabase = to.namespace().level(1);
    Preconditions.checkArgument(fromDatabase.equals(toDatabase),
        "Cannot move view between databases: from=%s and to=%s", fromDatabase, toDatabase);

    String fromViewName = from.name();
    String toViewName = to.name();
    if (!fromViewName.equals(toViewName)) {
      dbClient.getApi().renameTable(fromCatalog, fromDatabase, fromViewName, toViewName);
    }
  }

  protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
    validateTableIdentifier(viewIdentifier);

    String catalog = viewIdentifier.namespace().level(0);
    String database = viewIdentifier.namespace().level(1);

    DatabaseDto dbInfo = dbClient.getApi().getDatabase(catalog, database,
        false, /* omit user metadata */
        false /* omit table names */);

    if (dbInfo.getUri() != null) {
      return dbInfo.getUri() + "/" + viewIdentifier.name();
    }

    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format("%s/%s.db/%s", warehouseLocation, database, viewIdentifier.name());
  }

  @Override
  public void drop(String viewIdentifier) {
    TableIdentifier ident = toCatalogTableIdentifier(viewIdentifier);
    validateTableIdentifier(ident);
    dbClient.getApi().deleteTable(ident.namespace().level(0), ident.namespace().level(1), ident.name());
  }

  private Client newClient() {
    return MetacatUtil.newClient(appName, metacatHost, conf);
  }

  protected BaseMetastoreViewOperations newViewOps(TableIdentifier viewIdentifier) {
    validateTableIdentifier(viewIdentifier);

    String catalog = viewIdentifier.namespace().level(0);
    String database = viewIdentifier.namespace().level(1);

    return new MetacatViewClientOps(conf, newClient(), catalog, database, viewIdentifier.name());
  }

  private static void validateTableIdentifier(TableIdentifier viewIdentifier) {
    Preconditions.checkArgument(viewIdentifier.hasNamespace(), "viewIdentifier should have namespace");
    Preconditions.checkArgument(
        viewIdentifier.namespace().levels().length >= 2,
        "namespace should have catalog and schema");
  }
}
