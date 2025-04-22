package com.netflix.iceberg.properties;

import com.netflix.iceberg.metacat.DefinitionMetadata;
import com.netflix.iceberg.metacat.OperationContext;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class handles encoding to/from properties/metacat json
 *
 */
public class JanitorPropertiesHandler implements JsonPropertiesHandler {
    /*
     * "definitionMetadata": {
     *   "lifetime": {
     *     "user": "rblue",
     *     "days": -1 or 120,
     *     "snapshotTTL": 3
     *   },
     *   "data_hygiene": {
     *     "delete_method": "manually deleted" or "by partition column",
     *     "delete_column": "utc_date",
     *     "hour_column": "utc_hour" <-- not supported
     *   }
     * }
     */
    // json field names
    private static final String LIFETIME = "lifetime";
    private static final String USER = "user";
    private static final String SNAPSHOT_TTL = "snapshotTTL";
    private static final String DATA_HYGIENE = "data_hygiene";
    private static final String METHOD = "delete_method";
    private static final String COLUMN = "delete_column";
    private static final String DAYS = "days";

    // property names
    public static final String DATA_TTL_PROP = "janitor.data-ttl-days";
    public static final String DATA_TTL_COLUMN_PROP = "janitor.data-ttl-column";
    public static final String DATA_TTL_METHOD_PROP = "janitor.data-ttl-method";
    public static final String SNAPSHOT_TTL_PROP = "janitor.snapshot-ttl-days";

    private static final String DATA_TTL_MANUAL = "manually deleted";
    private static final String DATA_TTL_REPLACED_DAILY = "replaced daily";
    private static final String DATA_TTL_BY_PARTITION_COLUMN = "by partition column";
    private static final String DATA_TTL_BY_DATE_COLUMN = "by date column";
    private static final String DATA_TTL_BY_ACCOUNT_ID = "delete by account_id";
    private static final Set<String> VALID_TTL_METHODS = Sets.newHashSet(
            DATA_TTL_MANUAL, DATA_TTL_REPLACED_DAILY, DATA_TTL_BY_PARTITION_COLUMN, DATA_TTL_BY_DATE_COLUMN,
            DATA_TTL_BY_ACCOUNT_ID);

    public static final String SET_DEFAULT_SNAPSHOT_TTL = "netflix.janitors.set-default-snapshot-ttl";
    public static final Integer DEFAULT_SNAPSHOT_TTL_DAYS = 3; /* Store 3 days worth of snapshots by default */

    @Override
    public String prefix() {
        return "janitor.";
    }

    @Override
    public Map<String, String> fromJson(TableIdentifier tableIdentifier, ObjectNode jsonNode) {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        if (jsonNode.has(LIFETIME)) {
            JsonNode lifetime = jsonNode.get(LIFETIME);
            DefinitionMetadata.copyNumber(lifetime, DAYS, properties, DATA_TTL_PROP);
            DefinitionMetadata.copyNumber(lifetime, SNAPSHOT_TTL, properties, SNAPSHOT_TTL_PROP);
        }

        if (jsonNode.has(DATA_HYGIENE)) {
            JsonNode dataHygiene = jsonNode.get(DATA_HYGIENE);
            DefinitionMetadata.copyString(dataHygiene, METHOD, properties, DATA_TTL_METHOD_PROP);
            DefinitionMetadata.copyString(dataHygiene, COLUMN, properties, DATA_TTL_COLUMN_PROP);
        }
        return properties.build();
    }

    @Override
    public ObjectNode toJson(TableIdentifier tableIdentifier, Map<String, String> properties, OperationContext context) {
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        String ttlUpdate = properties.get(DATA_TTL_PROP);
        String snapshotTtlUpdate = properties.get(SNAPSHOT_TTL_PROP);
        if (ttlUpdate != null || snapshotTtlUpdate != null) {
            ObjectNode lifetime = JsonNodeFactory.instance.objectNode();

            if (ttlUpdate != null) {
                try {
                    lifetime.put(DAYS, Long.parseLong(ttlUpdate));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value for %s: %s", DATA_TTL_PROP, ttlUpdate));
                }
            }

            if (snapshotTtlUpdate != null) {
                try {
                    lifetime.put(SNAPSHOT_TTL, Long.parseLong(snapshotTtlUpdate));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value for %s: %s", SNAPSHOT_TTL, snapshotTtlUpdate));
                }
            }

            lifetime.put(USER, context.getUser());
            jsonNode.replace(LIFETIME, lifetime);
        }

        String ttlMethod = properties.get(DATA_TTL_METHOD_PROP);
        String ttlColumn = properties.get(DATA_TTL_COLUMN_PROP);
        if (ttlMethod != null || ttlColumn != null) {
            ObjectNode dataHygiene = JsonNodeFactory.instance.objectNode();

            if (ttlMethod != null) {
                String ttlMethodLower = ttlMethod.toLowerCase(Locale.ROOT);
                Preconditions.checkArgument(VALID_TTL_METHODS.contains(ttlMethodLower),
                        "Invalid value for %s: %s (not in %s)", DATA_TTL_METHOD_PROP, ttlMethod, VALID_TTL_METHODS);
                dataHygiene.put(METHOD, ttlMethodLower);
            }

            if (ttlColumn != null) {
                dataHygiene.put(COLUMN, ttlColumn);
            }

            jsonNode.replace(DATA_HYGIENE, dataHygiene);
        }
        return jsonNode;
    }
}
