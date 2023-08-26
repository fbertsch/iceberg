package com.netflix.iceberg.security;

import com.netflix.s3authsts.common.rest.StsCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.time.Instant;
import java.util.function.Supplier;

public class RefreshableStsProvider implements SerializableAwsCredentialsProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshableStsProvider.class);
  private final int refreshIfExpireInSecs;
  private final String resource;
  private volatile StsCredentials stsCredentials;
  private final Supplier<StsCredentials> stsCredentialsRefresher;

  public RefreshableStsProvider(int refreshIfExpireInSecs, String resource, StsCredentials stsCredentials, Supplier<StsCredentials> stsCredentialsRefresher) {
    this.refreshIfExpireInSecs = refreshIfExpireInSecs;
    this.resource = resource;
    this.stsCredentials = stsCredentials;
    this.stsCredentialsRefresher = stsCredentialsRefresher;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    // Refresh if expiring soon, and only once if shared by threads
    synchronized (stsCredentials) {
      int randomizedWindow = (int) (refreshIfExpireInSecs * Math.random());
      if (getExpiration().isBefore(Instant.now().plusSeconds(randomizedWindow))) {
        LOG.info("Refreshing STS credentials for " + resource + ", expiring/ed at " + getExpiration());
        stsCredentials = stsCredentialsRefresher.get();
      }
    }
    return AwsSessionCredentials.create(
        stsCredentials.getAccessKeyId(),
        stsCredentials.getSecretKey(),
        stsCredentials.getSessionToken());
  }

  @Override
  public Instant getExpiration() {
    return Instant.parse(stsCredentials.getExpiration());
  }
}
