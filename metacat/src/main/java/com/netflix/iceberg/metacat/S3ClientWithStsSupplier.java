package com.netflix.iceberg.metacat;

import com.netflix.iceberg.security.SecurityContext;
import com.netflix.iceberg.security.SerializableAwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;


public class S3ClientWithStsSupplier extends S3ClientSupplier {
  private SecurityContext securityContext;

  public S3ClientWithStsSupplier(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @Override
  public S3Client get() {
    SerializableAwsCredentialsProvider credentialsProvider = securityContext.getAwsCredentialsProvider();
    S3ClientBuilder builder = S3Client.builder();
    ClientOverrideConfiguration conf = S3Config.getOverrideConfig(s3UserAgentProvider.getUserAgentString());
    return builder
        .httpClient(ApacheHttpClient.create())
        .region(Region.of(region))
        .credentialsProvider(credentialsProvider)
        .overrideConfiguration(conf)
        .build();
  }

  @Override
  public S3ClientSupplier cloneWithRegion(String region) {
    S3ClientSupplier s3ClientSupplier = new S3ClientWithStsSupplier(securityContext);
    s3ClientSupplier.region = region;
    return s3ClientSupplier;
  }
}
