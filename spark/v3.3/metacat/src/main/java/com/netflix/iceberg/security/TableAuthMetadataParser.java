package com.netflix.iceberg.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.iceberg.util.StreamingJsonParser;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class TableAuthMetadataParser {

    private static final Logger LOG = LoggerFactory.getLogger(TableAuthMetadata.class);

    private static final String TABLE_UUID = "table-uuid";
    private static final String LOCATION = "location";
    private static final String PROPERTIES = "properties";

    public static TableAuthMetadata get(InputFile file) {
        try {
            try {
                return fromStreamingJson(file);
            } catch (Exception e) {
                LOG.warn("Error parsing metadata {} using streaming parser. Reverting to Object Mapper", file.location(), e);
                return fromJson(file);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to read file %s and fetch table auth metadata", file), e);
        }
    }

    private static InputStream getInputStream(InputFile file) throws IOException {
        TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromFileName(file.location());
        return codec == TableMetadataParser.Codec.GZIP ? new GZIPInputStream(file.newStream()) : file.newStream();
    }

    private static TableAuthMetadata fromStreamingJson(InputFile file) throws IOException {
        try (InputStream is = getInputStream(file); StreamingJsonParser jsonParser = new StreamingJsonParser(is)) {
            Map<String, Class<?>> fields = Maps.newHashMap();
            fields.put(TABLE_UUID, String.class);
            fields.put(LOCATION, String.class);
            fields.put(PROPERTIES, Map.class);

            Map<String, Object> result = jsonParser.parse(fields);

            return new TableAuthMetadata(
                    (String) result.get(TABLE_UUID),
                    (String) result.get(LOCATION),
                    (Map<String, String>) result.get(PROPERTIES)
            );
        }
    }

    private static TableAuthMetadata fromJson(InputFile file) throws IOException {
        try (InputStream is = getInputStream(file)) {
            JsonNode node = JsonUtil.mapper().readValue(is, JsonNode.class);
            return new TableAuthMetadata(
                    node.get(TABLE_UUID).asText(),
                    node.get(LOCATION).asText(),
                    JsonUtil.getStringMap(PROPERTIES, node)
            );
        }
    }
}
