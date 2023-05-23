package org.apache.iceberg.spark;

import org.apache.spark.sql.RuntimeConfig;

public class NetflixConf {

  public static final String GENIE_ID_CONF = "spark.genie.id";
  public static final String GENIE_GROUPING_CONF = "spark.genie.grouping";
  public static final String GENIE_TAGS_CONF = "spark.genie.tags";
  public static final String MAD_ID_CONF = "spark.mad.id";
  public static final String NETFLIX_SPARK_VERSION_CONF = "spark.netflix.app.version";

  private final String genieId;
  private final String schedulerWorkflowId;
  private final String schedulerStepId;
  private final String schedulerName;
  private final String schedulerCluster;
  private final String netflixSparkVersion;
  private final String madId;

  public NetflixConf(RuntimeConfig sessionConf) {
    this.genieId = sessionConf.get(GENIE_ID_CONF, null);

    String genieGrouping = sessionConf.get(GENIE_GROUPING_CONF, null);
    this.schedulerWorkflowId = parseSchedulerWorkflowId(genieGrouping);
    this.schedulerStepId = parseSchedulerStepId(genieGrouping);

    String tags = sessionConf.get(GENIE_TAGS_CONF, null);
    this.schedulerName = getValueFromTags(tags, "scheduler.name");
    this.schedulerCluster = getValueFromTags(tags, "scheduler.cluster");
    this.netflixSparkVersion = sessionConf.get(NETFLIX_SPARK_VERSION_CONF, null);
    this.madId = sessionConf.get(MAD_ID_CONF, null);
  }

  public String genieId() {
    return genieId;
  }

  public String schedulerWorkflowId() {
    return schedulerWorkflowId;
  }

  public String schedulerStepId() {
    return schedulerStepId;
  }

  public String schedulerName() {
    return schedulerName;
  }

  public String schedulerCluster() {
    return schedulerCluster;
  }

  public String madId() {
    return madId;
  }

  public String netflixSparkVersion() {
    return netflixSparkVersion;
  }

  private String parseSchedulerWorkflowId(String genieGrouping) {
    if (genieGrouping != null && genieGrouping.contains(":")) {
      String[] parts = genieGrouping.split(":");
      if (parts.length >= 1) {
        return parts[0];
      }
    }
    return null;
  }

  private String parseSchedulerStepId(String genieGrouping) {
    if (genieGrouping != null && genieGrouping.contains(":")) {
      String[] parts = genieGrouping.split(":");
      if (parts.length == 2) {
        return parts[1];
      }
    }
    return null;
  }

  private String getValueFromTags(String tags, String confKey) {
    if (tags != null) {
      String[] tagsArr = tags.split(",");
      for (String tag : tagsArr) {
        if (tag.startsWith(confKey)) {
          String[] tagParts = tag.split(":");
          if (tagParts.length == 2) {
            return tagParts[1];
          }
        }
      }
    }
    return null;
  }
}
