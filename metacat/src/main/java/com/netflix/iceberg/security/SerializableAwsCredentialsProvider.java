package com.netflix.iceberg.security;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.io.Serializable;
import java.time.Instant;

public interface SerializableAwsCredentialsProvider extends AwsCredentialsProvider, Serializable {
    Instant getExpiration();
}
