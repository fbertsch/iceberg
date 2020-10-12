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

package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldMetadataParser {
  private static final Logger LOG = LoggerFactory.getLogger(FieldMetadataParser.class);

  private FieldMetadataParser() {
  }

  public static String toJson(Map<Integer, String> metadata) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory()
          .createGenerator(writer)
          .setPrettyPrinter(new MinimalPrettyPrinter());
      toJson(metadata, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write field metadata json for: %s", metadata), e);
    }
  }

  private static void toJson(Map<Integer, String> metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    for (Map.Entry<Integer, String> fieldEntry : metadata.entrySet()) {
      generator.writeFieldName(String.valueOf(fieldEntry.getKey()));
      generator.writeRawValue(fieldEntry.getValue());
    }
    generator.writeEndObject();
  }

  public static Map<Integer, String> fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readTree(json));
    } catch (IOException | IllegalArgumentException e) {
      // this also catches NumberFormatException, which is an IllegalArgumentException
      LOG.warn("Failed to convert field metadata from json: " + json, e);
      return null;
    }
  }

  private static Map<Integer, String> fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null && !node.isNull() && node.isObject(),
        "Cannot parse non-object field metadata: %s", node);

    ImmutableMap.Builder<Integer, String> fieldMetadata = ImmutableMap.builder();

    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> fieldEntry = fields.next();
      Integer id = Integer.valueOf(fieldEntry.getKey());
      JsonNode metadataNode = fieldEntry.getValue();
      Preconditions.checkArgument(metadataNode != null && !metadataNode.isNull() && metadataNode.isObject(),
          "Cannot parse non-object metadata: %s", metadataNode);

      fieldMetadata.put(id, metadataNode.toString());
    }

    return fieldMetadata.build();
  }
}
