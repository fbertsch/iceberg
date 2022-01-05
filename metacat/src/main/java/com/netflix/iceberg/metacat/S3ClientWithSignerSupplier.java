package com.netflix.iceberg.metacat;

import com.netflix.iceberg.security.SecurityContext;
import com.netflix.s3authsign.signer.NflxAuthS3SignerRest;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.SIGNER;

public class S3ClientWithSignerSupplier implements SerializableSupplier<S3Client> {
  public S3ClientWithSignerSupplier(String signerAppName, SecurityContext securityContext) {
    this.signerAppName = signerAppName;
    this.securityContext = securityContext;
  }

  private String signerAppName;
  private SecurityContext securityContext;

  @Override
  public S3Client get() {
    NflxAuthS3SignerRest signer = NflxAuthS3SignerRest.builder()
        .withHost(securityContext.signerServiceUrl())
        .withService(signerAppName) // signer app name (for SSL)
        .build();
    securityContext.configure(signer);
    return S3Client.builder()
        .httpClient(UrlConnectionHttpClient.create())
        .region(Region.of(securityContext.signerRegion()))
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .putAdvancedOption(SIGNER, signer)
            .build()).build();
  }
}
