package com.netflix.iceberg.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class StreamingJsonParser implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJsonParser.class);
    private JsonParser jsonParser;

    public StreamingJsonParser(InputStream inputStream) throws IOException {
        this.jsonParser = JsonUtil.mapper().getFactory().createParser(inputStream);
    }

    public Map<String, Object> parse(Map<String, Class<?>> fields) throws IOException {
        try {
            Map<String, Object> result = new HashMap<>(fields.size());

            if (this.jsonParser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Invalid json file");
            }

            while (result.size() != fields.size() && this.jsonParser.nextToken() != null) {
                if (this.jsonParser.currentToken() == JsonToken.FIELD_NAME) {
                    String fieldName = this.jsonParser.currentName();
                    this.jsonParser.nextToken();
                    if (fieldName != null && fields.containsKey(fieldName)) {
                        result.put(fieldName, this.jsonParser.readValueAs(fields.get(fieldName)));
                    } else {
                        this.jsonParser.skipChildren();
                    }
                }
            }

            return result;
        } finally {
            close();
        }
    }

    public void close() {
        try {
            this.jsonParser.close();
        } catch (Exception e) {
            LOG.warn("Error while trying to close the json stream", e);
        }
    }
}
