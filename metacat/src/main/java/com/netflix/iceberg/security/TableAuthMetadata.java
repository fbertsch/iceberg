package com.netflix.iceberg.security;

import org.apache.iceberg.Table;

import java.util.Map;

public class TableAuthMetadata {

    private final String tableUuid;
    private final String location;
    private final Map<String, String> properties;

    public TableAuthMetadata(String tableUuid, String location, Map<String, String> properties) {
        this.tableUuid = tableUuid;
        this.location = location;
        this.properties = properties;
    }

    public String getTableUuid() {
        return tableUuid;
    }

    public String getLocation() {
        return location;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static TableAuthMetadata get(Table table) {
        return new TableAuthMetadata(
                table.uuid(),
                table.location(),
                table.properties()
        );
    }
}
