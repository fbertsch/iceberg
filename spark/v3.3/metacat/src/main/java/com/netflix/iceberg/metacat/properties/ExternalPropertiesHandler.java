package com.netflix.iceberg.metacat.properties;

import com.netflix.iceberg.metacat.OperationContext;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

public interface ExternalPropertiesHandler {
    /**
     * This should return the prefix specific to this handler
     * @return
     */
    public String prefix();

    public Map<String, String> loadProperties(TableIdentifier tableIdentifier);

    public void saveProperties(TableIdentifier tableIdentifier, Map<String, String> properties, OperationContext context);
}
