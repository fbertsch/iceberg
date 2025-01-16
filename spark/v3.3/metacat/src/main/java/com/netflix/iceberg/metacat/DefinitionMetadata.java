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

import com.netflix.bdp.security.authorization.AuthPolicy;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.CHILD_TABLE_UUID;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.MIGRATED_DATA_LOCATION;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.PARENT_CHILD_RELATION_INFO;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.PARENT_TABLE_NAME;
import static com.netflix.iceberg.metacat.MetacatIcebergCatalog.PARENT_TABLE_UUID;
import static com.netflix.iceberg.metacat.MetacatUtil.NETFLIX_OWNER;
import static com.netflix.iceberg.metacat.MetacatUtil.OWNER;
import static com.netflix.iceberg.metacat.MetacatUtil.USER_ID;

public class DefinitionMetadata {
  // security properties
  static final String SECURE_FLAG = "secure";
  static final String AUTH_POLICY = "auth_policy";


  private static final String COMMENT_PROP = "comment";
  private static final String VTTS_TIMESTAMP_SECONDS = "vtts.timestamp-utc-seconds";
  private static final String VTTS_TRIGGER_METHOD = "vtts.trigger-method";

  private static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet(
      COMMENT_PROP, VTTS_TIMESTAMP_SECONDS, VTTS_TRIGGER_METHOD, SECURE_FLAG, AUTH_POLICY);

  // "definitionMetadata": {
  //   "data_dependency": {
  // 	   "valid_thru_utc_ts": 1584457200, <-- in seconds
  //     "valid_thru_utc_ts_trigger": "manual" or "automatic" or "streaming",
  //     "valid_thru_utc_ts_updated_by": "rblue",
  //     "valid_thru_utc_ts_updated_at": 1584461001, <-- in seconds
  //     "valid_thru_utc_ts_genie_job_id": "genie-id" <-- not supported, no Genie ID here
  //   }
  // }
  private static final String VTTS_PREFIX = "vtts.";
  private static final String DATA_DEPENDENCY = "data_dependency";
  private static final String VTTS_SECONDS = "valid_thru_utc_ts";
  private static final String VTTS_TRIGGER = "valid_thru_utc_ts_trigger";
  private static final String VTTS_UPDATE_USER = "valid_thru_utc_ts_updated_by";
  private static final String VTTS_UPDATED_AT_SECONDS = "valid_thru_utc_ts_updated_at";
  private static final String MANUAL = "manual";
  private static final String AUTOMATIC = "automatic";
  private static final String STREAMING = "streaming";
  private static final String NEVER = "never";
  private static final String SPECIAL = "special";
  private static final Set<String> VALID_VTTS_TRIGGERS = Sets.newHashSet(MANUAL, AUTOMATIC, STREAMING, NEVER, SPECIAL);

  // "definitionMetadata": {
  //   "table_description": "Table doc string"
  // }
  private static final String DESCRIPTION = "table_description";

  public static String getMigratedDataLoc(ObjectNode definitionMetadata) {
    if(definitionMetadata.hasNonNull(MIGRATED_DATA_LOCATION)) {
      return definitionMetadata.get(MIGRATED_DATA_LOCATION).asText().replace("s3n://", "s3://").trim();
    }
    return null;
  }

  /**
   * Returns the userId stored under the owner object in the definitionMetadata
   * @return Returns userId string if the field exists otherwise return empty string
   */
  public static String getOwnerUserId(ObjectNode definitionMetadata) {
    if (definitionMetadata != null && definitionMetadata.hasNonNull(OWNER)) {
      JsonNode definitionMetadataOwner = definitionMetadata.get(OWNER);
      return definitionMetadataOwner.get(USER_ID).asText("");
    }
    return "";
  }

  static boolean isSecure(ObjectNode definitionMetadata) {
   return definitionMetadata != null && definitionMetadata.has(SECURE_FLAG) && definitionMetadata.get(SECURE_FLAG).asBoolean();
  }

  static void setAuthPolicy(ObjectNode definitionMetadata, AuthPolicy authPolicy) {
    definitionMetadata.put(AUTH_POLICY, authPolicy.name());
  }

  static void markSecure(ObjectNode definitionMetadata) {
    definitionMetadata.put(SECURE_FLAG, true);
  }

  static boolean isAuthPolicyPermissive(ObjectNode definitionMetadata) {
    String authPolicyStr = DefinitionMetadata.getAuthPolicy(definitionMetadata);
    return authPolicyStr != null && authPolicyStr.equals(AuthPolicy.PERMISSIVE.name());
  }

  static String getAuthPolicy(ObjectNode definitionMetadata) {
    if(definitionMetadata.hasNonNull(AUTH_POLICY)) {
      return definitionMetadata.get(AUTH_POLICY).asText();
    }
    return null;
  }

  public static String getAsText(JsonNode node, String field) {
    if (node != null && node.hasNonNull(field)) {
      return node.get(field).asText();
    }
    return null;
  }

  static boolean isReservedProperty(String property) {
    return RESERVED_PROPERTIES.contains(property);
  }

  static Map<String, String> reservedProperties(ObjectNode definitionMetadata) {
    if (definitionMetadata != null) {
      ImmutableMap.Builder<String, String> reservedProperties = ImmutableMap.builder();

      if (definitionMetadata.has(DATA_DEPENDENCY)) {
        JsonNode dataDependency = definitionMetadata.get(DATA_DEPENDENCY);
        copyNumber(dataDependency, VTTS_SECONDS, reservedProperties, VTTS_TIMESTAMP_SECONDS);
        copyString(dataDependency, VTTS_TRIGGER, reservedProperties, VTTS_TRIGGER_METHOD);
      }

      copyString(definitionMetadata, DESCRIPTION, reservedProperties, COMMENT_PROP);

      return reservedProperties.build();
    }

    return ImmutableMap.of();
  }

  static ObjectNode buildDefinitionMetadata(TableMetadata base, TableMetadata current) {
    ObjectNode metadata = JsonNodeFactory.instance.objectNode();
    addOwner(metadata, base, current);
    addDescription(metadata, base, current);
    addVTTSProperties(metadata, base, current);
    addFlinkWatermarkProperties(metadata, base, current);
    addSecurityProperties(metadata, base, current);
    return metadata;
  }

  private static void addOwner(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    if (current.properties().containsKey(OWNER) || current.properties().containsKey(NETFLIX_OWNER)) {
      metadata.set(
              OWNER,
              JsonNodeFactory.instance.objectNode().put("userId", MetacatUtil.getUser(current))
      );
    }
  }

  /**
   * Merge children nodes of a and b, value from b is used if a field appears in both.
   */
  static ObjectNode overwriteMerge(ObjectNode a, ObjectNode b) {
    return a.deepCopy().setAll(b.deepCopy());
  }

  private static void addDescription(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), COMMENT_PROP);

    String description = updates.get(COMMENT_PROP);
    if (description != null) {
      metadata.put(DESCRIPTION, description);
    }
  }

  private static void addVTTSProperties(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), VTTS_PREFIX);

    if (updates.isEmpty()) {
      return;
    }

    boolean isUpdate = base != null;
    ObjectNode dataDependency = null;

    String triggerMethod = updates.get(VTTS_TRIGGER_METHOD);
    if (triggerMethod != null) {
      dataDependency = JsonNodeFactory.instance.objectNode();
      String triggerMethodLower = triggerMethod.toLowerCase(Locale.ROOT);
      Preconditions.checkArgument(VALID_VTTS_TRIGGERS.contains(triggerMethodLower),
          "Invalid value for %s: %s (not in %s)", VTTS_TRIGGER_METHOD, triggerMethod, VALID_VTTS_TRIGGERS);
      dataDependency.put(VTTS_TRIGGER, triggerMethodLower);
    }

    String vttsTimestamp = updates.get(VTTS_TIMESTAMP_SECONDS);
    // only set VTTS if this is an update and not when the table is created. this avoids failures when VTTS is copied
    // over by CREATE TABLE ... LIKE with a trigger method that is not "manual". otherwise, the check is correct.
    if (isUpdate && vttsTimestamp != null) {
      String currentTriggerMethod = triggerMethod != null ? triggerMethod : base.properties().get(VTTS_TRIGGER_METHOD);
      Preconditions.checkArgument(MANUAL.equals(currentTriggerMethod),
          "Cannot manually set VTTS: trigger method is %s", currentTriggerMethod);

      long timestampSeconds = Long.parseLong(vttsTimestamp);
      Preconditions.checkArgument(timestampSeconds < 10000000000L,
          "Invalid value for %s: %s (not in seconds)", VTTS_TIMESTAMP_SECONDS, vttsTimestamp);

      String previousTimestamp = base.properties().get(VTTS_TIMESTAMP_SECONDS);
      long previousTimestampSeconds = previousTimestamp != null ? Long.parseLong(previousTimestamp) : Long.MIN_VALUE;
      Preconditions.checkArgument(timestampSeconds >= previousTimestampSeconds,
          "Invalid value for %s: %s (not later than %s)",
          VTTS_TIMESTAMP_SECONDS, vttsTimestamp, previousTimestampSeconds);

      if (dataDependency == null) {
        dataDependency = JsonNodeFactory.instance.objectNode();
      }

      dataDependency.put(VTTS_SECONDS, String.valueOf(timestampSeconds));
      dataDependency.put(VTTS_UPDATE_USER, MetacatUtil.getUser(current));
      dataDependency.put(VTTS_UPDATED_AT_SECONDS, System.currentTimeMillis() / 1_000);
    }

    if (dataDependency != null) {
      metadata.put(DATA_DEPENDENCY, dataDependency);
    }
  }

  private static final String FLINK_WATERMARK_PREFIX = "flink.watermark.";

  private static void addFlinkWatermarkProperties(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), FLINK_WATERMARK_PREFIX);

    if (updates.isEmpty()) {
      return;
    }

    ObjectNode watermarks = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> update : updates.entrySet()) {
      watermarks.put(
          update.getKey().replace(FLINK_WATERMARK_PREFIX, ""),
          Long.parseLong(update.getValue()));
    }

    metadata.put("flink.watermarks", watermarks);
  }

  private static void addSecurityProperties(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    if (current.properties().containsKey(SECURE_FLAG)) {
      metadata.put(SECURE_FLAG, current.properties().get(SECURE_FLAG));
    }
  }

  private static Map<String, String> changedProperties(Map<String, String> base, Map<String, String> current,
                                               String prefix) {
    Map<String, String> result = Maps.newHashMap();
    Set<Map.Entry<String, String>> baseSet = base != null ? base.entrySet() : Collections.emptySet();

    for (Map.Entry<String, String> entry : current.entrySet()) {
      // forward the properties that are not in the base set (changed) and match the prefix
      if (!baseSet.contains(entry) && entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }

  public static void copyString(JsonNode node, String jsonProperty,
                                 ImmutableMap.Builder<String, String> builder, String property) {
    if (node != null && node.isObject() && node.has(jsonProperty)) {
      JsonNode value = node.get(jsonProperty);
      if (value != null && !value.isNull() && value.isTextual()) {
        builder.put(property, value.asText());
      }
    }
  }

  public static void copyNumber(JsonNode node, String jsonProperty,
                                 ImmutableMap.Builder<String, String> builder, String property) {
    if (node != null && node.isObject() && node.has(jsonProperty)) {
      JsonNode value = node.get(jsonProperty);
      if (value != null && !value.isNull() && value.isNumber()) {
        builder.put(property, value.asText());
      }
    }
  }

  public static void setParentChildRelationship(ObjectNode meta, String rootTableName, String rootTableUuid, String childTableUuid) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put(PARENT_TABLE_NAME, rootTableName);
    node.put(PARENT_TABLE_UUID, rootTableUuid);
    node.put(CHILD_TABLE_UUID, childTableUuid);

    meta.put(PARENT_CHILD_RELATION_INFO, node);
  }
}
