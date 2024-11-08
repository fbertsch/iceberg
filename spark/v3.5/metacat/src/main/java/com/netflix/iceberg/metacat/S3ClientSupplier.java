package com.netflix.iceberg.metacat;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3Client;

public abstract class S3ClientSupplier implements SerializableSupplier<S3Client> {
  protected transient S3UserAgentProvider s3UserAgentProvider;
  private static final String DEFAULT_REGION = "us-east-1";
  protected String region = DEFAULT_REGION;

  public void init(Configuration conf) {
    s3UserAgentProvider = S3UserAgentProvider.of(conf);
  }

  public S3ClientSupplier cloneWithRegion(String region) {
    throw new UnsupportedOperationException();
  }
}
