package com.netflix.iceberg.metacat;

import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockServices
{
    private static final ObjectMapper objectMapper;
    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    private MockServices() {}

    public static Client mockMetacat(String getTableJson, String updateTableString)
            throws JsonProcessingException
    {
        // mock the metacat response
        MetacatApi mockMetacatApi = mock(MetacatApi.class);
        when(mockMetacatApi.getTable(
                "prodhive",
                "vault",
                "oca_session_f",
                true,
                true,
                false,
                false,
                true
        )).thenReturn(
                objectMapper.readValue(
                        getTableJson,
                        TableDto.class));
        when(mockMetacatApi.updateTable(
                eq("prodhive"),
                eq("vault"),
                eq("oca_session_f"),
                any(TableDto.class)
        )).thenReturn(
                objectMapper.readValue(
                        updateTableString,
                        TableDto.class));
        Client mockClient = mock(Client.class);
        when(mockClient.getApi()).thenReturn(mockMetacatApi);
        return mockClient;
    }

    public static HttpClient mockNdcHttpClient(String getMetadataJson)
            throws IOException
    {
        // mock ndc response
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse response = new BasicClosableHttpResponse(
                new BasicStatusLine(HttpVersion.HTTP_1_1, 200, null));
        response.setEntity(new StringEntity(getMetadataJson, StandardCharsets.UTF_8));
        when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(response);
        return mockHttpClient;
    }

    public static HttpClient mockNdcDgsClient(String getMetadataJson)
            throws IOException
    {
        // mock ndc response
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse response = new BasicClosableHttpResponse(
                new BasicStatusLine(HttpVersion.HTTP_1_1, 200, null));
        response.setEntity(new StringEntity(getMetadataJson, StandardCharsets.UTF_8));
        when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(response);
        return mockHttpClient;
    }

    private static class BasicClosableHttpResponse extends BasicHttpResponse
            implements CloseableHttpResponse
    {
        public BasicClosableHttpResponse(StatusLine statusline)
        {
            super(statusline);
        }

        @Override
        public void close()
                throws IOException
        {

        }
    }
}
