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

package com.netflix.iceberg.metacat;

import com.netflix.metacat.client.api.MetacatV1;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.spectator.ipc.IpcLogEntry;
import com.netflix.spectator.ipc.IpcLogger;
import com.netflix.spectator.ipc.IpcProtocol;
import com.netflix.spectator.ipc.IpcResult;
import com.netflix.spectator.ipc.IpcStatus;
import java.util.List;
import java.util.concurrent.Callable;

public class MetacatApi implements MetacatV1 {

  private final MetacatV1 metacatV1;

  private final IpcLogger ipcLogger;

  public static Builder builder() {return new Builder();}

  private MetacatApi(MetacatV1 metacatV1, IpcLogger ipcLogger) {
    this.metacatV1 = metacatV1;
    this.ipcLogger = ipcLogger;
  }

  private MetacatIpcLogEntry createIpcLogEntry(String endpoint) {
    return MetacatIpcLogEntry.create(ipcLogger)
        .withOwner("iceberg-metacat")
        .withProtocol(IpcProtocol.http_1)
        .withEndpoint(endpoint);
  }

  @Override
  public void createCatalog(CreateCatalogDto createCatalogDto) {
    createIpcLogEntry("createCatalog")
        .withCatalog(createCatalogDto.getName().getCatalogName())
        .run(() -> metacatV1.createCatalog(createCatalogDto));
  }

  @Override
  public void createDatabase(
      String catalogName,
      String databaseName,
      DatabaseCreateRequestDto databaseCreateRequestDto) {
    createIpcLogEntry("createDatabase")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .run(() -> metacatV1.createDatabase(catalogName, databaseName, databaseCreateRequestDto));
  }

  @Override
  public TableDto createTable(String catalogName, String databaseName, String tableName, TableDto tableDto) {
    return createIpcLogEntry("createTable")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .call(() -> metacatV1.createTable(catalogName, databaseName, tableName, tableDto));
  }

  @Override
  public TableDto createMView(
      String catalogName, String databaseName, String tableName, String viewName, Boolean snapshot, String filter) {
    return createIpcLogEntry("createMView")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .withView(viewName)
        .call(() -> metacatV1.createMView(catalogName, databaseName, tableName, viewName, snapshot, filter));
  }

  @Override
  public void deleteDatabase(String catalogName, String databaseName) {
    createIpcLogEntry("deleteDatabase")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .run(() -> metacatV1.deleteDatabase(catalogName, databaseName));
  }

  @Override
  public TableDto deleteTable(String catalogName, String databaseName, String tableName) {
    return createIpcLogEntry("deleteTable")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .call(() -> metacatV1.deleteTable(catalogName, databaseName, tableName));
  }

  @Override
  public TableDto deleteMView(String catalogName, String databaseName, String tableName, String viewName) {
    return createIpcLogEntry("deleteMView")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .withView(viewName)
        .call(() -> metacatV1.deleteMView(catalogName, databaseName, tableName, viewName));
  }

  @Override
  public CatalogDto getCatalog(String catalogName) {
    return createIpcLogEntry("getCatalog")
        .withCatalog(catalogName)
        .call(() -> metacatV1.getCatalog(catalogName));
  }

  @Override
  public List<CatalogMappingDto> getCatalogNames() {
    return createIpcLogEntry("getCatalogNames")
        .call(() -> metacatV1.getCatalogNames());
  }

  @Override
  public DatabaseDto getDatabase(
      String catalogName,
      String databaseName,
      Boolean includeUserMetadata,
      Boolean includeTableNames) {
    return createIpcLogEntry("getDatabase")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .call(() -> metacatV1.getDatabase(catalogName, databaseName, includeUserMetadata, includeTableNames));
  }

  @Override
  public TableDto getTable(
      String catalogName,
      String databaseName,
      String tableName,
      Boolean includeInfo,
      Boolean includeDefinitionMetadata,
      Boolean includeDataMetadata,
      Boolean includeInfoDetails,
      Boolean includeMetadataLocationOnly) {
    return createIpcLogEntry("getTable")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .call(
            () -> metacatV1.getTable(
                catalogName,
                databaseName,
                tableName,
                includeInfo,
                includeDefinitionMetadata,
                includeDataMetadata,
                includeInfoDetails,
                includeMetadataLocationOnly));
  }

  @Override
  public void tableExists(String catalogName, String databaseName, String tableName) {
    createIpcLogEntry("tableExists")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .run(() -> metacatV1.tableExists(catalogName, databaseName, tableName));
  }

  @Override
  public List<QualifiedName> getTableNames(String catalogName, String databaseName, Integer limit) {
    return createIpcLogEntry("getTableNames")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .call(() -> metacatV1.getTableNames(catalogName, databaseName, limit));
  }

  @Override
  public List<QualifiedName> getTableNames(String catalogName, String databaseName, String filter, Integer limit) {
    return createIpcLogEntry("getTableNames")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .call(() -> metacatV1.getTableNames(catalogName, databaseName, filter, limit));
  }

  @Override
  public List<NameDateDto> getMViews(String catalogName) {
    return createIpcLogEntry("getMViews")
        .withCatalog(catalogName)
        .call(() -> metacatV1.getMViews(catalogName));
  }

  @Override
  public List<NameDateDto> getMViews(String catalogName, String databaseName, String tableName) {
    return createIpcLogEntry("getMViews")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .call(() -> metacatV1.getMViews(catalogName, databaseName, tableName));
  }

  @Override
  public TableDto getMView(String catalogName, String databaseName, String tableName, String viewName) {
    return createIpcLogEntry("getMView")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .withView(viewName)
        .call(() -> metacatV1.getMView(catalogName, databaseName, tableName, viewName));
  }

  @Override
  public void renameTable(String catalogName, String databaseName, String tableName, String newTableName) {
    createIpcLogEntry("renameTable")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .run(() -> metacatV1.renameTable(catalogName, databaseName, tableName, newTableName));
  }

  @Override
  public void updateCatalog(String catalogName, CreateCatalogDto createCatalogDto) {
    createIpcLogEntry("updateCatalog")
        .withCatalog(catalogName)
        .run(() -> metacatV1.updateCatalog(catalogName, createCatalogDto));
  }

  @Override
  public void updateDatabase(
      String catalogName,
      String databaseName,
      DatabaseCreateRequestDto databaseCreateRequestDto) {
    createIpcLogEntry("updateDatabase")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .run(() -> metacatV1.updateDatabase(catalogName, databaseName, databaseCreateRequestDto));
  }

  @Override
  public TableDto updateMView(
      String catalogName,
      String databaseName,
      String tableName,
      String viewName,
      TableDto tableDto) {
    return createIpcLogEntry("updateMView")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .withView(viewName)
        .call(() -> metacatV1.updateMView(catalogName, databaseName, tableName, viewName, tableDto));
  }

  @Override
  public TableDto updateTable(String catalogName, String databaseName, String tableName, TableDto tableDto) {
    return createIpcLogEntry("updateTable")
        .withCatalog(catalogName)
        .withDatabase(databaseName)
        .withTable(tableName)
        .call(() -> metacatV1.updateTable(catalogName, databaseName, tableName, tableDto));
  }

  private static class MetacatIpcLogEntry {
    private final IpcLogEntry ipcLogEntry;

    public static MetacatIpcLogEntry create(IpcLogger ipcLogger) {
      IpcLogEntry ipcLogEntry = ipcLogger.createClientEntry()
          .withProtocol("metacat");
      return new MetacatIpcLogEntry(ipcLogEntry);
    }

    private MetacatIpcLogEntry(IpcLogEntry ipcLogEntry) {
      this.ipcLogEntry = ipcLogEntry;
    }

    public MetacatIpcLogEntry withOwner(String owner) {
      ipcLogEntry.withOwner(owner);
      return this;
    }

    public MetacatIpcLogEntry withProtocol(IpcProtocol protocol) {
      ipcLogEntry.withProtocol(protocol);
      return this;
    }

    public MetacatIpcLogEntry withEndpoint(String endpoint) {
      ipcLogEntry.withEndpoint(endpoint);
      return this;
    }

    public MetacatIpcLogEntry withCatalog(String catalogName) {
      ipcLogEntry.addTag("metacat.catalog", catalogName);
      return this;
    }

    public MetacatIpcLogEntry withDatabase(String databaseName) {
      ipcLogEntry.addTag("metacat.database", databaseName);
      return this;
    }

    public MetacatIpcLogEntry withTable(String tableName) {
      // Remove this tag due to high cardinality.
      // See https://netflix.slack.com/archives/C0RAMNX8U/p1705440801880579.
      // ipcLogEntry.addTag("metacat.table", tableName);
      return this;
    }

    public MetacatIpcLogEntry withView(String viewName) {
      // Remove this tag due to high cardinality.
      // See https://netflix.slack.com/archives/C0RAMNX8U/p1705440801880579.
      // ipcLogEntry.addTag("metacat.view", viewName);
      return this;
    }

    public void run(Runnable runnable) {
      ipcLogEntry.markStart();
      try {
        runnable.run();
        ipcLogEntry.markEnd().withResult(IpcResult.success).withStatus(IpcStatus.success).log();
      } catch (Exception e) {
        ipcLogEntry.markEnd().withResult(IpcResult.failure).withException(e).log();
        throw e;
      }
    }

    public <V> V call(Callable<V> callable) {
      ipcLogEntry.markStart();
      try {
        V value = callable.call();
        ipcLogEntry.markEnd().withResult(IpcResult.success).withStatus(IpcStatus.success).log();
        return value;
      } catch (RuntimeException e) {
        ipcLogEntry.markEnd().withResult(IpcResult.failure).withException(e).log();
        throw e;
      } catch (Exception e) {
        ipcLogEntry.markEnd().withResult(IpcResult.failure).withException(e).log();
        throw new RuntimeException(e);
      }
    }
  }

  public static final class Builder {
    private MetacatV1 metacatV1;
    private IpcLogger ipcLogger;

    private Builder() {}

    public Builder withMetacatV1(MetacatV1 metacatV1) {
      this.metacatV1 = metacatV1;
      return this;
    }

    public Builder withIpcLogger(IpcLogger ipcLogger) {
      this.ipcLogger = ipcLogger;
      return this;
    }

    public MetacatApi build() {return new MetacatApi(metacatV1, ipcLogger);}
  }
}
