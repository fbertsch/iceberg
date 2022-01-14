package com.netflix.iceberg.metacat;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class S3ClientWithRoleSupplier extends S3ClientSupplier {

  private String roleArn;
  private int sessionDurationSecs;
  private S3UserAgentProvider s3UserAgentProvider;

  public S3ClientWithRoleSupplier(String roleArn, int sessionDurationSecs, S3UserAgentProvider s3UserAgentProvider) {
    this.roleArn = roleArn;
    this.sessionDurationSecs = sessionDurationSecs;
    this.s3UserAgentProvider = s3UserAgentProvider;
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
              .region(Region.of(region)).build())
          .refreshRequest(assumeRoleRequest).build();
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    S3ClientBuilder builder = S3Client.builder();
    ClientOverrideConfiguration conf = S3Config.getOverrideConfig(s3UserAgentProvider.getUserAgentString());
    return builder
        .credentialsProvider(credentialsProvider)
        .httpClient(UrlConnectionHttpClient.create())
        .region(Region.of(region))
        .overrideConfiguration(conf)
        .build();
  }
}
