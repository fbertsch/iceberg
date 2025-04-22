package com.netflix.iceberg.metacat;

public class OperationContext {
    private final String user;
    public OperationContext(String user) {
        this.user = user;
    }
    public String getUser() {
        return user;
    }
}
