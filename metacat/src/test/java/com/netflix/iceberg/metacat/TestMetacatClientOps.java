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

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.iceberg.metacat.MetacatClientOps;
import com.netflix.iceberg.properties.JanitorPropertiesHandler;
import com.netflix.iceberg.properties.NdcPropertiesHandler;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.client.api.MetacatV1;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.shaded.com.google.common.collect.ImmutableMap;
import com.netflix.metacat.shaded.com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static com.netflix.iceberg.metacat.MockServices.mockMetacat;
import static com.netflix.iceberg.metacat.MockServices.mockNdcHttpClient;
import static com.netflix.iceberg.properties.JanitorPropertiesHandler.DATA_TTL_COLUMN_PROP;
import static com.netflix.iceberg.properties.JanitorPropertiesHandler.DATA_TTL_METHOD_PROP;
import static com.netflix.iceberg.properties.JanitorPropertiesHandler.DATA_TTL_PROP;
import static com.netflix.iceberg.properties.JanitorPropertiesHandler.SNAPSHOT_TTL_PROP;
import static com.netflix.iceberg.properties.NdcPropertiesHandler.NDC_UPDATE_ENABLED_CONF;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestMetacatClientOps {
    @Test
    public void testSortOrderFromString() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get()),
                Types.NestedField.required(3, "timestamp", Types.TimestampType.withoutZone()),
                Types.NestedField.required(4, "date", Types.DateType.get()),
                Types.NestedField.required(5, "decimal", Types.DecimalType.of(10, 2))
        );
        String sortOrderStr = "bucket(4, id) ASC NULLS FIRST, truncate(3, data) DESC NULLS LAST, year(date) ASC NULLS FIRST, month(timestamp) DESC NULLS LAST, decimal DESC NULLS FIRST";
        SortOrder sortOrder = MetacatClientOps.sortOrderFromString(schema, sortOrderStr);

        List<SortField> sortFields = sortOrder.fields();

        SortField bucketField = sortFields.get(0);
        Assert.assertEquals(Transforms.bucket(4), bucketField.transform());
        Assert.assertEquals(SortDirection.ASC, bucketField.direction());
        Assert.assertEquals(NullOrder.NULLS_FIRST, bucketField.nullOrder());
        Assert.assertEquals(1, bucketField.sourceId());
        Assert.assertEquals("bucket[4](1) ASC NULLS FIRST", bucketField.toString());

        SortField truncateField = sortFields.get(1);
        Assert.assertEquals(Transforms.truncate(3), truncateField.transform());
        Assert.assertEquals(SortDirection.DESC, truncateField.direction());
        Assert.assertEquals(NullOrder.NULLS_LAST, truncateField.nullOrder());
        Assert.assertEquals(2, truncateField.sourceId());
        Assert.assertEquals("truncate[3](2) DESC NULLS LAST", truncateField.toString());

        SortField yearField = sortFields.get(2);
        Assert.assertEquals(Transforms.year(), yearField.transform());
        Assert.assertEquals(SortDirection.ASC, yearField.direction());
        Assert.assertEquals(NullOrder.NULLS_FIRST, yearField.nullOrder());
        Assert.assertEquals(4, yearField.sourceId());
        Assert.assertEquals("year(4) ASC NULLS FIRST", yearField.toString());

        SortField monthField = sortFields.get(3);
        Assert.assertEquals(Transforms.month(), monthField.transform());
        Assert.assertEquals(SortDirection.DESC, monthField.direction());
        Assert.assertEquals(NullOrder.NULLS_LAST, monthField.nullOrder());
        Assert.assertEquals(3, monthField.sourceId());
        Assert.assertEquals("month(3) DESC NULLS LAST", monthField.toString());

        SortField identityField = sortFields.get(4);
        Assert.assertEquals(Transforms.identity(), identityField.transform());
        Assert.assertEquals(SortDirection.DESC, identityField.direction());
        Assert.assertEquals(NullOrder.NULLS_FIRST, identityField.nullOrder());
        Assert.assertEquals(5, identityField.sourceId());
        Assert.assertEquals("identity(5) DESC NULLS FIRST", identityField.toString());

    }

    @Test
    public void testReadNdcProperties() throws IOException {
        String metacatGetTableString = Resources.toString(
                Resources.getResource("properties/metacat.json"),
                StandardCharsets.UTF_8);
        String s3MetadataJson = Resources.toString(
                Resources.getResource("metadata.json"),
                StandardCharsets.UTF_8);
        TableMetadata mockMetadata = TableMetadataParser.fromJson("s3_file_location", s3MetadataJson);
        String ndcGetMetadataJson = Resources.toString(
                Resources.getResource("ndc.json"),
                StandardCharsets.UTF_8);

        try (MockedStatic<TableMetadataParser> mockParser = mockStatic(TableMetadataParser.class)) {
            // mock s3 metadata
            mockParser.when(() -> TableMetadataParser.read(any(), anyString())).thenReturn(mockMetadata);
            // mock ndc response
            HttpClient mockHttpClient = mockNdcHttpClient(ndcGetMetadataJson);
            // mock metacat
            Client mockClient = mockMetacat(metacatGetTableString, metacatGetTableString);
            Configuration conf = new Configuration(false);
            conf.setBoolean(NDC_UPDATE_ENABLED_CONF, true);
            NdcPropertiesHandler ndcHandler = new NdcPropertiesHandler(conf, () -> mockHttpClient);
            MetacatClientOps clientOps = new MetacatClientOps(
                    conf,
                    mockClient,
                    TableIdentifier.parse("prodhive.vault.oca_session_f"),
                    Collections.singletonList(ndcHandler),
                    Collections.emptyList());
            clientOps.doRefresh();
            TableMetadata tableMetadata = clientOps.current();
            Assert.assertEquals(
                    "yes",
                    tableMetadata.property("netflix.ndc.pi", "defaultValue"));
            Assert.assertEquals(
                    "consumer",
                    tableMetadata.property("netflix.ndc.business_unit", "defaultValue"));
        }
    }

    @Test
    public void testWriteNdcProperties() throws IOException {
        String metacatGetTableString = Resources.toString(
                Resources.getResource("properties/metacat.json"),
                StandardCharsets.UTF_8);
        String s3MetadataJson = Resources.toString(
                Resources.getResource("metadata.json"),
                StandardCharsets.UTF_8);
        TableMetadata mockMetadata = TableMetadataParser.fromJson("s3_file_location", s3MetadataJson);
        String ndcGetMetadataJson = Resources.toString(
                Resources.getResource("ndc.json"),
                StandardCharsets.UTF_8);

        try (MockedStatic<TableMetadataParser> mockParser = mockStatic(TableMetadataParser.class)) {
            // mock s3 metadata
            mockParser.when(() -> TableMetadataParser.read(any(), anyString())).thenReturn(mockMetadata);
            // mock ndc response
            HttpClient mockHttpClient = mockNdcHttpClient(ndcGetMetadataJson);
            // mock metacat
            Client mockClient = mockMetacat(metacatGetTableString, metacatGetTableString);
            Configuration conf = new Configuration(false);
            conf.setBoolean(NDC_UPDATE_ENABLED_CONF, true);
            NdcPropertiesHandler ndcHandler = new NdcPropertiesHandler(conf, () -> mockHttpClient);
            MetacatClientOps clientOps = new MetacatClientOps(
                    conf,
                    mockClient,
                    TableIdentifier.parse("prodhive.vault.oca_session_f"),
                    Collections.singletonList(ndcHandler),
                    Collections.emptyList());
            clientOps.doRefresh();
            ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
            verify(mockHttpClient, times(1)).execute(requestCaptor.capture());
            Assert.assertEquals(
                    "GET",
                    requestCaptor.getValue().getMethod());
            Assert.assertEquals(
                    URI.create("https://ndc.cluster.us-east-1.prod.cloud.netflix.net:8443/api/v0/metadata?name=ndc%3A%2F%2Fhive%3Aprod%2Fprodhive%2Fvault%2Foca_session_f"),
                    requestCaptor.getValue().getURI());
            TableMetadata tableMetadata = clientOps.current();
            Assert.assertEquals(
                    tableMetadata.properties().toString(),
                    "yes",
                    tableMetadata.property("netflix.ndc.pi", "defaultValue"));
            Assert.assertEquals(
                    tableMetadata.properties().toString(),
                    "consumer",
                    tableMetadata.property("netflix.ndc.business_unit", "defaultValue"));

            // send and update
            TableMetadata updated = tableMetadata.replaceProperties(
                    ImmutableMap.of(
                            "netflix.ndc.pi", "no",
                            "netflix.ndc.business_unit", "ads",
                            "netflix.ndc.labels", "ok_to_delete"
                    )
            );
            clientOps.doCommit(tableMetadata, updated);

            // verify what we tried to call the backend with
            verify(mockHttpClient, times(2)).execute(requestCaptor.capture());
            HttpPut capturedPut = (HttpPut) requestCaptor.getValue();
            Assert.assertEquals(
                    "PUT",
                    capturedPut.getMethod());
            Assert.assertEquals(
                    URI.create("https://ndc.cluster.us-east-1.prod.cloud.netflix.net:8443/api/v0/metadata"),
                    capturedPut.getURI());
            JsonNode actualRequestPayload = JsonUtil.mapper().readTree(capturedPut.getEntity().getContent());
            JsonNode expectedRequestPayload = JsonUtil.mapper().readTree(
                    "{\"dlmDataCategoryTags\":{\"pi\":\"no\",\"business_unit\":\"ads\"},"
                                + "\"labels\":[\"ok_to_delete\"],"
                                + "\"name\":\"ndc://hive:prod/prodhive/vault/oca_session_f\"}");
            Assert.assertEquals(
                    expectedRequestPayload,
                    actualRequestPayload);
        }
    }

    @Test
    public void testReadJanitorProperties() throws IOException {
        String metacatGetTableString = Resources.toString(
                Resources.getResource("properties/metacat.json"),
                StandardCharsets.UTF_8);
        String s3MetadataJson = Resources.toString(
                Resources.getResource("metadata.json"),
                StandardCharsets.UTF_8);
        TableMetadata mockMetadata = TableMetadataParser.fromJson("s3_file_location", s3MetadataJson);

        try (MockedStatic<TableMetadataParser> mockParser = mockStatic(TableMetadataParser.class)) {
            // mock s3 metadata
            mockParser.when(() -> TableMetadataParser.read(any(), anyString())).thenReturn(mockMetadata);
            // mock metacat
            Client mockClient = mockMetacat(metacatGetTableString, metacatGetTableString);
            Configuration conf = new Configuration(false);
            JanitorPropertiesHandler janitorHandler = new JanitorPropertiesHandler();
            MetacatClientOps clientOps = new MetacatClientOps(
                    conf,
                    mockClient,
                    TableIdentifier.parse("prodhive.vault.oca_session_f"),
                    Collections.emptyList(),
                    Collections.singletonList(janitorHandler));
            clientOps.doRefresh();
            TableMetadata tableMetadata = clientOps.current();
            Assert.assertEquals(
                    "defaultValue",
                    tableMetadata.property(DATA_TTL_PROP, "defaultValue"));
            Assert.assertEquals(
                    "utc_date",
                    tableMetadata.property(DATA_TTL_COLUMN_PROP, "defaultValue"));
            Assert.assertEquals(
                    "by partition column",
                    tableMetadata.property(DATA_TTL_METHOD_PROP, "defaultValue"));
            Assert.assertEquals(
                    "30",
                    tableMetadata.property(SNAPSHOT_TTL_PROP, "defaultValue"));
        }
    }

    @Test
    public void testWriteJanitorProperties() throws IOException {
        String metacatGetTableString = Resources.toString(
                Resources.getResource("properties/metacat.json"),
                StandardCharsets.UTF_8);
        String s3MetadataJson = Resources.toString(
                Resources.getResource("metadata.json"),
                StandardCharsets.UTF_8);
        TableMetadata mockMetadata = TableMetadataParser.fromJson("s3_file_location", s3MetadataJson);

        try (MockedStatic<TableMetadataParser> mockParser = mockStatic(TableMetadataParser.class)) {
            // mock s3 metadata
            mockParser.when(() -> TableMetadataParser.read(any(), anyString())).thenReturn(mockMetadata);
            // mock metacat
            Client mockClient = mockMetacat(metacatGetTableString, metacatGetTableString);
            Configuration conf = new Configuration(false);
            JanitorPropertiesHandler janitorHandler = new JanitorPropertiesHandler();
            MetacatClientOps clientOps = new MetacatClientOps(
                    conf,
                    mockClient,
                    TableIdentifier.parse("prodhive.vault.oca_session_f"),
                    Collections.emptyList(),
                    Collections.singletonList(janitorHandler));
            clientOps.doRefresh();
            TableMetadata tableMetadata = clientOps.current();
            Assert.assertEquals(
                    "defaultValue",
                    tableMetadata.property(DATA_TTL_PROP, "defaultValue"));
            Assert.assertEquals(
                    "utc_date",
                    tableMetadata.property(DATA_TTL_COLUMN_PROP, "defaultValue"));
            Assert.assertEquals(
                    "by partition column",
                    tableMetadata.property(DATA_TTL_METHOD_PROP, "defaultValue"));
            Assert.assertEquals(
                    "30",
                    tableMetadata.property(SNAPSHOT_TTL_PROP, "defaultValue"));

            // send and update
            TableMetadata updated = tableMetadata.replaceProperties(
                    ImmutableMap.of(
                            DATA_TTL_PROP, "10",
                            DATA_TTL_COLUMN_PROP, "not_utc_date",
                            DATA_TTL_METHOD_PROP, "manually deleted",
                            SNAPSHOT_TTL_PROP, "7"
                    )
            );
            clientOps.doCommit(tableMetadata, updated);

            // verify what we tried to call the backend with
            ArgumentCaptor<TableDto> requestCaptor = ArgumentCaptor.forClass(TableDto.class);
            MetacatV1 metacatApi = mockClient.getApi();
            verify(metacatApi, times(1)).updateTable(
                    eq("prodhive"),
                    eq("vault"),
                    eq("oca_session_f"),
                    requestCaptor.capture()
            );
            TableDto capturedTable = requestCaptor.getValue();

            Assert.assertEquals(
                    "10",
                    capturedTable.getDefinitionMetadata().get("lifetime").get("days").asText());
            Assert.assertEquals(
                    "7",
                    capturedTable.getDefinitionMetadata().get("lifetime").get("snapshotTTL").asText());
            Assert.assertEquals(
                    "not_utc_date",
                    capturedTable.getDefinitionMetadata().get("data_hygiene").get("delete_column").asText());
            Assert.assertEquals(
                    "manually deleted",
                    capturedTable.getDefinitionMetadata().get("data_hygiene").get("delete_method").asText());
        }
    }
}
