package com.netflix.iceberg.metacat;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import com.netflix.metatron.ipc.security.MetatronSslContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.iceberg.util.JsonUtil;

public class NdcUtil {
  public static final String NDC_UPDATE_ENABLED_CONF = "netflix.ndc.enabled";
  public static final String NDC_PROD_PREFIX = "netflix.ndc.";
  public static final String NDC_URL = "https://ndc.cluster.us-east-1.prod.cloud.netflix.net:8443/api/v0/metadata";
  public static final Set<String> NDC_CATEGORY_KEYS = ImmutableSet.of("pi", "business_unit");

  private static HttpClient httpClient;

  private static String getQualifiedNameStr(String catalogName, String dbName, String tableName) {
    return String.format("ndc://hive:prod:us-east-1/%s/%s/%s", catalogName, dbName, tableName);
  }

  private static synchronized HttpClient getHttpClient() {
    if (httpClient == null) {
      SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(
          MetatronSslContext.forClient("ndc"),
          null,
          null,
          NoopHostnameVerifier.INSTANCE /* hostname verification is handled internally by MetatronSslContext */);

      httpClient = HttpClientBuilder.create()
          .setSSLSocketFactory(sslFactory)
          .build();
    }
    return httpClient;
  }

  public static void updateNdc(
      String catalogName, String dbName, String tableName,
      Map<String, String> ndcProps) {

    if (NDC_CATEGORY_KEYS.stream().anyMatch(key -> ndcProps.containsKey(key))) {

      String ndcName = getQualifiedNameStr(catalogName, dbName, tableName);

      Map<String, String> dlmCategoryTags = ndcProps.entrySet().stream()
          .filter(e -> NDC_CATEGORY_KEYS.contains(e.getKey()))
          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

      HttpPut request = new HttpPut(NDC_URL);

      try {
        request.setEntity(new StringEntity(
            "{\"dlmDataCategoryTags\":" + JsonUtil.mapper().writeValueAsString(dlmCategoryTags) +
                ",\"name\":\"" + ndcName + "\"}",
            ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = (CloseableHttpResponse) getHttpClient().execute(request)) {
          int statusCode = response.getStatusLine().getStatusCode();
          if (statusCode < 200 || statusCode >= 300) {
            throw new RuntimeException(
                "Failed to update ndc: table = " + catalogName + "." + dbName + "." + tableName
                    + ", ndcProps = " + ndcProps
                    + ", response = " + statusCode + " | " + EntityUtils.toString(request.getEntity()));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String catalog = "prodhive";
    String db = "tjiang";
    String tbl = "ice1";
    Map<String, String> props = new HashMap<>();
    props.put("pi", "abd");
    updateNdc(catalog, db, tbl, props);
  }
}