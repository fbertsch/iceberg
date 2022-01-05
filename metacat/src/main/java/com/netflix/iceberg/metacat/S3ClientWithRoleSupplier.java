package com.netflix.iceberg.metacat;

import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class S3ClientWithRoleSupplier implements SerializableSupplier<S3Client> {

  private String roleArn;
  private int sessionDurationSecs;

  public S3ClientWithRoleSupplier(String roleArn, int sessionDurationSecs) {
    this.roleArn = roleArn;
    this.sessionDurationSecs = sessionDurationSecs;
  }

  @Override
  public S3Client get() {
    final AwsCredentialsProvider credentialsProvider;

    if(roleArn != null) {
      AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
          .roleArn(roleArn)
          .roleSessionName("iceberg-s3fileio")
          .durationSeconds(sessionDurationSecs)
          .build();

      credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
          .stsClient(StsClient.builder()
              .httpClient(UrlConnectionHttpClient.create())
              .region(Region.US_EAST_1).build())
          .refreshRequest(assumeRoleRequest).build();
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    return S3Client.builder()
        .credentialsProvider(credentialsProvider)
        .httpClient(UrlConnectionHttpClient.create())
        .region(Region.US_EAST_1)
        .build();
  }
}
