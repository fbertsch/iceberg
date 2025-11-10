package com.netflix.iceberg.properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.iceberg.metacat.OperationContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import com.netflix.metatron.ipc.security.MetatronSslContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Arrays;
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
  public static final String NDC_PROD_PREFIX = "netflix.ndc.";
  public static final String NDC_URL = "https://ndc.cluster.us-east-1.prod.cloud.netflix.net:8443/api/v0/metadata";
  public static final String NDC_DGS_URL = "https://ndcdgs.vip.us-east-1.prod.cloud.netflix.net:8443/graphql";
  public static final Set<String> NDC_CATEGORY_KEYS = ImmutableSet.of("pi", "business_unit");
  public static final String NDC_LABELS_KEY = "labels";
  public static final String NDC_NAME_KEY = "name";
  private static final String NDC_DATA_CATEGORY_TAGS = "dataCategoryTags";
  private static final String NDC_DATA_CATEGORY_TAG = "tag";
  private static final String NDC_DATA_CATEGORY_CODE = "code";
  public static final String NDC_DLM_DATA_CATEGORY_TAGS_KEY = "dlmDataCategoryTags";
  public static final String TESTHIVE_CATALOG_NAME = "testhive";

  private static HttpClient httpClient;
  private static HttpClient dgsClient;

  private static String getQualifiedNameStr(TableIdentifier tableIdentifier) {
    Namespace namespace = tableIdentifier.namespace();
    if (namespace.length() != 2) {
      throw new IllegalArgumentException("Invalid TableIdentifier, namespace.length() != 2, : " + tableIdentifier);
    }
    String catalog = namespace.level(0);
    String schema = namespace.level(1);
    String table = tableIdentifier.name();
    String env = TESTHIVE_CATALOG_NAME.equals(catalog) ? "test" : "prod";
    return String.format("ndc://hive:%s/%s/%s/%s", env, catalog, schema, table).toLowerCase();
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

  private static synchronized HttpClient getDgsClient() {
    if (dgsClient == null) {
      SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(
              MetatronSslContext.forClient("ndcdgs"),
              null,
              null,
              NoopHostnameVerifier.INSTANCE);

      dgsClient = HttpClientBuilder.create()
              .setSSLSocketFactory(sslFactory)
              .build();
    }
    return dgsClient;
  }

  public static Map<String, String> readNdc(TableIdentifier tableIdentifier, Supplier<HttpClient> httpClientSupplier) {
    String ndcName = getQualifiedNameStr(tableIdentifier);
    HttpUriRequest request = RequestBuilder.get(NDC_URL).addParameter(NDC_NAME_KEY, ndcName).build();
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
          JsonNode dataCategoryTags = responseJson.path(0).path(NDC_DLM_DATA_CATEGORY_TAGS_KEY);
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
          Supplier<HttpClient> dgsClient
  ) {

    if (NDC_CATEGORY_KEYS.stream().anyMatch(ndcProps::containsKey)
        || ndcProps.containsKey(NDC_LABELS_KEY)) {

      String ndcName = getQualifiedNameStr(tableIdentifier);

      Map<String, String> dlmCategoryTags = ndcProps.entrySet().stream()
          .filter(e -> NDC_CATEGORY_KEYS.contains(e.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      Set<String> labels = Arrays.stream(ndcProps.getOrDefault(NDC_LABELS_KEY, "").split(","))
          .map(String::trim)
          .filter(label -> !label.isEmpty())
          .collect(Collectors.toSet());

      try {
        // Build GraphQL mutation query
        String mutation = "mutation UpdateDatasetMetadata($input: NDC_DatasetMetadataInput!) { "
                + "ndc_updateDatasetMetadata(input: $input) { void } }";

        // Build the input variables for the mutation
        ObjectNode input = JsonUtil.mapper().createObjectNode();
        input.put(NDC_NAME_KEY, ndcName);

        // Transform dlmCategoryTags map to array of tag objects
        if (!dlmCategoryTags.isEmpty()) {
          ArrayNode dataCategoryTagsArray = JsonUtil.mapper().createArrayNode();
          dlmCategoryTags.forEach((k, v) -> {
            ObjectNode dataCategory = JsonUtil.mapper().createObjectNode();
            dataCategory.set(NDC_DATA_CATEGORY_TAG, JsonUtil.mapper().valueToTree(k));
            dataCategory.set(NDC_DATA_CATEGORY_CODE, JsonUtil.mapper().valueToTree(v));
            dataCategoryTagsArray.add(dataCategory);
          });
          input.set(NDC_DATA_CATEGORY_TAGS,dataCategoryTagsArray);
        }

        if (!labels.isEmpty()) {
          input.set(NDC_LABELS_KEY, JsonUtil.mapper().valueToTree(labels));
        }

        // Build the GraphQL request payload
        ObjectNode variables = JsonUtil.mapper().createObjectNode();
        variables.set("input", input);

        ObjectNode graphqlRequest = JsonUtil.mapper().createObjectNode();
        graphqlRequest.put("query", mutation);
        graphqlRequest.set("variables", variables);

        HttpPost request = new HttpPost(NDC_DGS_URL);
        request.setEntity(new StringEntity(graphqlRequest.toString(), ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = (CloseableHttpResponse) dgsClient.get().execute(request)) {
          int statusCode = response.getStatusLine().getStatusCode();
          String responseBody = EntityUtils.toString(response.getEntity());

          if (statusCode < 200 || statusCode >= 300) {
            throw new RuntimeException(
                    "Failed to update ndc: table"
                            + ", ndcProps = " + ndcProps
                            + ", response = " + statusCode + " | " + responseBody);
          }

          // Check for GraphQL errors in the response
          JsonNode responseJson = JsonUtil.mapper().readTree(responseBody);
          if (responseJson.has("errors")) {
            throw new RuntimeException(
                    "Request error updating ndc: table"
                            + ", ndcProps = " + ndcProps
                            + ", errors = " + responseJson.get("errors").toString());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final Configuration conf;
  private final Supplier<HttpClient> httpClientSupplier;
  private final Supplier<HttpClient> dgsClientSupplier;

  public NdcPropertiesHandler(Configuration conf) {
      this(conf, NdcPropertiesHandler::getHttpClient, NdcPropertiesHandler::getDgsClient);
  }

  // visible for testing
  public NdcPropertiesHandler(Configuration conf, Supplier<HttpClient> httpClientSupplier, Supplier<HttpClient> dgsClientSupplier) {
      this.conf = conf;
      this.httpClientSupplier = httpClientSupplier;
      this.dgsClientSupplier = dgsClientSupplier;
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
        NdcPropertiesHandler.updateNdc(tableIdentifier, ndcProps, dgsClientSupplier);
      }
    }
  }
}