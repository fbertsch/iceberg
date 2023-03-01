/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.hive;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LocalHiveCatalog extends BaseMetastoreCatalog implements Closeable, SupportsNamespaces {

  private LocalHiveMetastore localHiveMetastore;
  private HiveCatalog hiveCatalog;

  private boolean closed;

  public LocalHiveCatalog() {
  }

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    Configuration conf = new Configuration();
    Map<String, String> newProperties = Maps.newHashMap(properties);

    try {
      String warehouseLocation = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "/tmp/hive-warehouse");
      Path warehouseLocationPath = FileSystem.get(conf).makeQualified(new Path(warehouseLocation));
      if (!"file".equals(warehouseLocationPath.getFileSystem(conf).getScheme())) {
        throw new IllegalArgumentException(
            String.format("Warehouse location for catalog '%s' should be local", name()));
      }
      conf.set("hive.metastore.warehouse.dir", warehouseLocationPath.toString());
      localHiveMetastore = new LocalHiveMetastore(warehouseLocationPath.toUri().getPath());
      localHiveMetastore.start();
      String metastoreUri = localHiveMetastore.hiveConf().get(HiveConf.ConfVars.METASTOREURIS.varname);
      newProperties.put(CatalogProperties.URI, metastoreUri);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    hiveCatalog = new HiveCatalog();
    hiveCatalog.setConf(conf);
    hiveCatalog.initialize(inputName, newProperties);
  }

  @Override
  public String name() {
    return hiveCatalog.name();
  }

  @Override
  public void close() {
    if (!closed) {
      try {
        if (localHiveMetastore != null) {
          localHiveMetastore.stop();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        closed = true;
      }
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return hiveCatalog.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return hiveCatalog.dropTable(identifier, purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    hiveCatalog.renameTable(from, to);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    hiveCatalog.createNamespace(namespace, metadata);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return hiveCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    return hiveCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return hiveCatalog.dropNamespace(namespace);
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    return hiveCatalog.setProperties(namespace, properties);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    return hiveCatalog.removeProperties(namespace, properties);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return hiveCatalog.newTableOps(tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return hiveCatalog.defaultWarehouseLocation(tableIdentifier);
  }
}
