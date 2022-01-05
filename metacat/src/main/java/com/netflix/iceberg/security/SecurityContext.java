package com.netflix.iceberg.security;

import com.netflix.s3authsign.signer.NflxAuthS3SignerRest;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SecurityContext implements Serializable {
  public static final String CREATE_TABLE_FLAG = "iceberg.create.table";
  public static final String SIGNER_RESOURCE = "resource";
  public static final String SIGNER_TOKEN = "token";

  private String signerServiceUrl;
  private String signerRegion;
  private final String tableIdentifier;
  private volatile String signerToken;
  private transient Supplier<String> e2eTokenSupplier;
  private boolean create;

  public SecurityContext(String tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public void configure(NflxAuthS3SignerRest signer) {
    signer.getExtensions().put(SIGNER_RESOURCE, tableIdentifier);

    if(signerToken != null) {
      signer.getExtensions().put(SIGNER_TOKEN, signerToken);
    }

    if(create) {
      signer.getExtensions().put(CREATE_TABLE_FLAG, "true");
    }

    signer.setExtensionConsumer((Consumer<Map<String,String>> & Serializable)(extensions) -> {
      if (extensions.containsKey(SIGNER_TOKEN)) {
        signerToken = extensions.get(SIGNER_TOKEN);
        signer.getExtensions().put(SIGNER_TOKEN, signerToken);
      }
    });

    if(e2eTokenSupplier != null) {
      signer.setE2eTokenSupplier(e2eTokenSupplier);
    }
  }

  public void create(boolean flag) {
    this.create = flag;
  }

  public String tableIdentifier() {
    return tableIdentifier;
  }

  public String signerRegion() {
    return signerRegion;
  }

  public void setSignerRegion(String signerRegion) {
    this.signerRegion = signerRegion;
  }

  public String signerServiceUrl() {
    return signerServiceUrl;
  }

  public void setSignerServiceUrl(String signerServiceUrl) {
    this.signerServiceUrl = signerServiceUrl;
  }

  public String signerToken() {
    return signerToken;
  }

  public void setSignerToken(String signerToken) {
    this.signerToken = signerToken;
  }

  public Supplier<String> e2eTokenSupplier() {
    return e2eTokenSupplier;
  }

  public void setE2eTokenSupplier(Supplier<String> e2eTokenSupplier) {
    this.e2eTokenSupplier = e2eTokenSupplier;
  }
}
