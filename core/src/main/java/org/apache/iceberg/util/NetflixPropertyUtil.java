package org.apache.iceberg.util;

public class NetflixPropertyUtil {
  public static boolean netflixJanitorAlwaysAllowGc() {
    return Boolean.valueOf(System.getProperty("netflix.janitor.alwaysAllowGc", "false"));
  }

  public static boolean netflixJanitorCleanExpiredFiles() {
    return Boolean.valueOf(System.getProperty("netflix.janitor.cleanExpiredFiles", "false"));
  }
}
