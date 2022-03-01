/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.metacat;

import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.shaded.feign.Request;
import com.netflix.metacat.shaded.feign.Retryer;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class MetacatUtil {
  private MetacatUtil() {
  }

  private static String getUser() {
    // Match the behavior of Hive's Utils.getUser. If HADOOP_USER_NAME is set, Hive will proxy using the session
    // credentials using doAs, so the effective user is HADOOP_USER_NAME. Otherwise, Hive will use the current
    // credentials to get a username.
    if (System.getenv("HADOOP_USER_NAME") != null) {
      return System.getenv("HADOOP_USER_NAME");
    }

    // Use the current credentials to get a username. This is the call made to determine user in Presto, too.
    try {
      return UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException e) {
      // use the USER environment variable instead
    }

    // If Hadoop environment credentials aren't available, try USER or the Java user.name system property.
    if (System.getenv("USER") != null) {
      return System.getenv("USER");
    } else {
      return System.getProperty("user.name");
    }
  }

  public static String getUser(TableMetadata tableMetadata) {
    // https://jira.netflix.net/browse/DPS-1156
    // Look for the table owner in table metadata, if set. Else rely on the user set in env variables.
    if (tableMetadata != null && tableMetadata.properties() != null
            && tableMetadata.properties().containsKey("owner")) {
      return tableMetadata.properties().get("owner");
    } else {
      return getUser();
    }
  }
  /**
   * Sync the metacat URI between options and conf.
   *
   * @return the metacat URI that is configured for this catalog
   */
  public static String syncMetacatUri(CaseInsensitiveStringMap options, Configuration conf) {
    String metacatUri = options.get("metacat-uri");
    if (metacatUri != null) {
      conf.set("netflix.metacat.host", metacatUri);
      return metacatUri;
    } else {
      return conf.get("netflix.metacat.host", null);
    }
  }

  public static String defaultTableLocation(Configuration conf, Client client,
                                            String catalog, String database, String tableName) {
    DatabaseDto dbInfo = client.getApi().getDatabase(catalog, database,
        false, /* omit user metadata */
        false /* omit table names */ );

    if (dbInfo.getUri() != null) {
      return dbInfo.getUri() + "/" + tableName;
    }

    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");

    return String.format("%s/%s.db/%s", warehouseLocation, database, tableName);
  }

  public static boolean dropTable(Client client, String catalog, String database, String tableName) {
    try {
      client.getApi().deleteTable(catalog, database, tableName);
      return true;
    } catch (MetacatNotFoundException e) {
      return false;
    }
  }

  public static String getJobId(Configuration conf) {
    return conf.get("genie.job.id");
  }

  public static Retryer getRetryer(Configuration conf) {
    long period = conf.getTimeDuration("netflix.metacat.retry.period", 1, TimeUnit.MINUTES);
    long maxPeriod = conf.getTimeDuration("netflix.metacat.retry.maxPeriod", 5, TimeUnit.MINUTES);
    int maxAttempts = conf.getInt("netflix.metacat.retry.maxAttempts", 3);
    return new Retryer.Default(TimeUnit.MINUTES.toMillis(period), TimeUnit.MINUTES.toMillis(maxPeriod), maxAttempts);
  }

  public static Request.Options getRequestOptions(Configuration conf) {
    long connectTimeoutMillis = conf.getInt("netflix.metacat.connectTimeoutMillis",
        (int) TimeUnit.MINUTES.toMillis(10));
    long readTimeoutMillis = conf.getInt("netflix.metacat.readTimeoutMillis",
        (int) TimeUnit.MINUTES.toMillis(30));
    return new Request.Options(
        connectTimeoutMillis, TimeUnit.MILLISECONDS, readTimeoutMillis, TimeUnit.MILLISECONDS, true);
  }

  public static Client newClient(String appName, String host, Configuration conf) {
    return Client.builder()
        .withClientAppName(appName)
        .withHost(host)
        .withJobId(getJobId(conf))
        .withUserName(getUser())
        .withDataTypeContext("hive")
        .withRetryer(getRetryer(conf))
        .withRequestOptions(getRequestOptions(conf))
        .build();
  }
}
