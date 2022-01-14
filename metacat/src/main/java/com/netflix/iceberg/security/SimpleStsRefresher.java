package com.netflix.iceberg.security;

import com.netflix.s3authsts.common.rest.StsCredentials;

import java.io.Serializable;
import java.util.function.Supplier;

public class SimpleStsRefresher implements Serializable, Supplier<StsCredentials> {
  private SecurityContext securityContext;

  public SimpleStsRefresher(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @Override
  public StsCredentials get() {
    return securityContext.loadStsCredentials();
  }
}
