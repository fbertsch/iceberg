package com.netflix.iceberg.metacat;

import com.netflix.iceberg.security.SecurityContext;
import com.netflix.s3authsign.signer.NflxAuthS3SignerRest;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.SIGNER;

public class S3ClientWithSignerSupplier extends S3ClientSupplier {

  private SecurityContext securityContext;

  public S3ClientWithSignerSupplier(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @Override
  public S3Client get() {
    NflxAuthS3SignerRest signer = NflxAuthS3SignerRest.builder()
        .withHost(securityContext.signerServiceHost())
        .withService(securityContext.signerAppName()) // signer app name (for SSL)
        .build();
    securityContext.configure(signer);

    S3ClientBuilder builder = S3Client.builder();
    ClientOverrideConfiguration conf = S3Config.getOverrideConfig(s3UserAgentProvider.getUserAgentString());
    conf = conf.toBuilder().putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer).build();
    return builder
        .httpClient(ApacheHttpClient.create())
        .region(Region.of(region))
        .overrideConfiguration(conf).build();
  }

  public S3ClientSupplier cloneWithRegion(String region) {
    S3ClientSupplier s3ClientSupplier = new S3ClientWithSignerSupplier(securityContext);
    s3ClientSupplier.region = region;
    return s3ClientSupplier;
  }
}
