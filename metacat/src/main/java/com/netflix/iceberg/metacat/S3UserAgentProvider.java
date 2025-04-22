package com.netflix.iceberg.metacat;

import com.netflix.bdp.s3fs.DefaultUserAgentProvider;
import com.netflix.bdp.s3fs.UserAgentProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3UserAgentProvider implements UserAgentProvider {
  private static final Logger LOG = LoggerFactory.getLogger(S3UserAgentProvider.class);
  private DefaultUserAgentProvider userAgentProvider;

  public static S3UserAgentProvider of(Configuration conf) {
    return new S3UserAgentProvider(conf);
  }

  private S3UserAgentProvider(Configuration conf) {
    userAgentProvider = new DefaultUserAgentProvider();
    userAgentProvider.setConf(conf);
  }

  @Override
  public String getUserAgentString() {
    try {
      return userAgentProvider.getUserAgentString();
    } catch (Exception e) {
      LOG.warn("Failed to generate s3 user agent string", e);
    }
    return null;
  }
}