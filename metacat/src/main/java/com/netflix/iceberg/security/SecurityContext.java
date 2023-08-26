package com.netflix.iceberg.security;

import com.netflix.s3authsign.signer.NflxAuthS3SignerRest;
import com.netflix.s3authsign.sts.NflxAuthS3StsRest;
import com.netflix.s3authsts.common.rest.StsCredentials;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityContext implements Serializable {
  public static final String CREATE_TABLE_FLAG = "iceberg.create.table";
  public static final String SIGNER_RESOURCE = "resource";
  public static final String SIGNER_TOKEN = "token";

  private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

  private String signerServiceHost;
  private String signerRegion;
  private String signerAppName;
  private final String tableIdentifier;
  private volatile String signerToken;
  private transient Supplier<String> e2eTokenSupplier;
  private boolean create;
  private String creationLocation;
  private SerializableAwsCredentialsProvider awsCredentialsProvider;

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

  private NflxAuthS3StsRest getNflxStsClient() {
    NflxAuthS3StsRest client = NflxAuthS3StsRest.builder()
            .withHost(signerServiceHost)
            .withService(signerAppName)
            .build();
    if(e2eTokenSupplier != null) {
      client.setE2eTokenSupplier(e2eTokenSupplier);
    }
    return client;
  }

  public StsCredentials loadStsCredentials() {
    return getNflxStsClient().getSts(tableIdentifier, null, creationLocation).getCredentials();
  }

  public void setupStsCredentialsProvider(int refreshIfExpireInSecs) {
    if(awsCredentialsProvider == null) {
      Supplier<StsCredentials> refresher = new SimpleStsRefresher(this);
      if (hasSpark()) {
        // loading the SparkStsRefresher via reflection
        try {
          Class<?> cl = Class.forName("com.netflix.iceberg.security.SparkStsRefresher");
          Constructor<?> cons = cl.getConstructor(SecurityContext.class);
          refresher = (Supplier<StsCredentials>) cons.newInstance(this);
        } catch (Exception e) {
          LOG.warn("Failed to load com.netflix.iceberg.security.SparkStsRefresher," +
                  " falling back to use SimpleStsRefresher..");
        }
      }
      this.awsCredentialsProvider = new RefreshableStsProvider(refreshIfExpireInSecs, tableIdentifier, loadStsCredentials(), refresher);
    }
  }

  public void create(boolean flag) {
    this.create = flag;
  }

  public boolean isCreate() {
    return create;
  }

  public String getCreationLocation() {
    return creationLocation;
  }

  public void setCreationLocation(String creationLocation) {
    this.creationLocation = creationLocation;
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

  public String signerAppName() {
    return signerAppName;
  }

  public void setSignerAppName(String signerAppName) {
    this.signerAppName = signerAppName;
  }

  public String signerServiceHost() {
    return signerServiceHost;
  }

  public void setSignerServiceHost(String signerServiceHost) {
    this.signerServiceHost = signerServiceHost;
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

  public SerializableAwsCredentialsProvider getAwsCredentialsProvider() {
    return awsCredentialsProvider;
  }

  public void setAwsCredentialsProvider(SerializableAwsCredentialsProvider awsCredentialsProvider) {
    this.awsCredentialsProvider = awsCredentialsProvider;
  }

  private Boolean hasSpark() {
    try {
      Class.forName("org.apache.spark.sql.SparkSession");
      return true;
    } catch(ClassNotFoundException ce){
      return false;
    }
  }
}
