package com.netflix.iceberg.metacat.properties;

import com.netflix.iceberg.metacat.OperationContext;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

public interface JsonPropertiesHandler {
    /**
     * This should return the prefix specific to this handler
     * @return
     */
    public String prefix();

    public Map<String, String> fromJson(TableIdentifier tableIdentifier, ObjectNode jsonNode);

    public ObjectNode toJson(TableIdentifier tableIdentifier, Map<String, String> properties, OperationContext context);
}
