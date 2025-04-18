package com.netflix.iceberg.metacat.properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.iceberg.metacat.OperationContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import com.netflix.metatron.ipc.security.MetatronSslContext;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.iceberg.util.JsonUtil;

public class NdcPropertiesHandler implements ExternalPropertiesHandler {
  public static final String NDC_UPDATE_ENABLED_CONF = "netflix.ndc.enabled";
  public static final String NDC_URL_CONF = "netflix.ndc.url";
  public static final String NDC_PROD_PREFIX = "netflix.ndc.";
  public static final String NDC_URL = "https://ndc.cluster.us-east-1.prod.cloud.netflix.net:8443/api/v0/metadata";
  public static final Set<String> NDC_CATEGORY_KEYS = ImmutableSet.of("pi", "business_unit");

  private static HttpClient httpClient;

  private static String getQualifiedNameStr(TableIdentifier tableIdentifier) {
    Namespace namespace = tableIdentifier.namespace();
    if (namespace.length() != 2) {
      throw new IllegalArgumentException("Invalid TableIdentifier, namespace.length() != 2, : " + tableIdentifier);
    }
    String catalog = namespace.level(0);
    String schema = namespace.level(1);
    String table = tableIdentifier.name();
    return String.format("ndc://hive:prod/%s/%s/%s", catalog, schema, table).toLowerCase();
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

  public static Map<String, String> readNdc(TableIdentifier tableIdentifier, Supplier<HttpClient> httpClientSupplier) {
    String ndcName = getQualifiedNameStr(tableIdentifier);
    HttpUriRequest request = RequestBuilder.get(NDC_URL).addParameter("name", ndcName).build();
    try (CloseableHttpResponse response = (CloseableHttpResponse) httpClientSupplier.get().execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      HttpEntity responseEntity = response.getEntity();
      if (statusCode < 200 || statusCode >= 300) {
        throw new RuntimeException(
                "Failed to fetch ndc properties: table = " + tableIdentifier
                        + ", response = " + statusCode + " | " + EntityUtils.toString(responseEntity));
      } else {
        JsonNode responseJson = JsonUtil.mapper().readTree(responseEntity.getContent());
        if (responseJson != null) {
          JsonNode dataCategoryTags = responseJson.path(0).path("dlmDataCategoryTags");
          Map<String, String> properties = new HashMap<>();
          for (String key : NDC_CATEGORY_KEYS) {
            if (dataCategoryTags.has(key)) {
              properties.put(NDC_PROD_PREFIX + key, dataCategoryTags.get(key).asText());
            }
          }
          return properties;
        } else {
          return Collections.emptyMap();
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to fetch ndc properties", ioe);
    }
  }

  public static void updateNdc(
      TableIdentifier tableIdentifier,
      Map<String, String> ndcProps,
      Supplier<HttpClient> httpClientSupplier) {

    if (NDC_CATEGORY_KEYS.stream().anyMatch(ndcProps::containsKey)) {

      String ndcName = getQualifiedNameStr(tableIdentifier);

      Map<String, String> dlmCategoryTags = ndcProps.entrySet().stream()
          .filter(e -> NDC_CATEGORY_KEYS.contains(e.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      HttpPut request = new HttpPut(NDC_URL);

      try {
        request.setEntity(new StringEntity(
            "{\"dlmDataCategoryTags\":" + JsonUtil.mapper().writeValueAsString(dlmCategoryTags) +
                ",\"name\":\"" + ndcName + "\"}",
            ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = (CloseableHttpResponse) httpClientSupplier.get().execute(request)) {
          int statusCode = response.getStatusLine().getStatusCode();
          if (statusCode < 200 || statusCode >= 300) {
            throw new RuntimeException(
                "Failed to update ndc: table = " + tableIdentifier
                    + ", ndcProps = " + ndcProps
                    + ", response = " + statusCode + " | " + EntityUtils.toString(request.getEntity()));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final Configuration conf;
  private final Supplier<HttpClient> httpClientSupplier;

  public NdcPropertiesHandler(Configuration conf) {
      this(conf, NdcPropertiesHandler::getHttpClient);
  }

  // visible for testing
  public NdcPropertiesHandler(Configuration conf, Supplier<HttpClient> httpClientSupplier) {
      this.conf = conf;
      this.httpClientSupplier = httpClientSupplier;
  }

  @Override
  public String prefix() {
    return NDC_PROD_PREFIX;
  }

  @Override
  public Map<String, String> loadProperties(TableIdentifier tableIdentifier) {
    return readNdc(tableIdentifier, httpClientSupplier);
  }

  @Override
  public void saveProperties(TableIdentifier tableIdentifier, Map<String, String> properties, OperationContext context) {
    if (conf.getBoolean(NDC_UPDATE_ENABLED_CONF, true) && properties != null) {
      Map<String, String> ndcProps =
              properties.entrySet()
                      .stream()
                      .collect(Collectors.toMap(e -> e.getKey().substring(NDC_PROD_PREFIX.length()), e -> e.getValue()));
      if (!ndcProps.isEmpty()) {
        NdcPropertiesHandler.updateNdc(tableIdentifier, ndcProps, httpClientSupplier);
      }
    }
  }
}