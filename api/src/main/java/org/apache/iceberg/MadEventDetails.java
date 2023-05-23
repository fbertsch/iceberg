package org.apache.iceberg;

public class MadEventDetails {
  private final String madId;
  private final String schedulerWorkflowId;
  private final String schedulerStepId;
  private final String schedulerName;
  private final String schedulerCluster;
  private final String genieId;
  private final String tableName;
  private final String netflixSparkVersion;

  public MadEventDetails(
      String tableName,
      String madId,
      String schedulerWorkflowId,
      String schedulerStepid,
      String schedulerName,
      String schedulerCluster,
      String genieId,
      String netflixSparkVersion) {
    this.tableName = tableName;
    this.madId = madId;
    this.schedulerWorkflowId = schedulerWorkflowId;
    this.schedulerStepId = schedulerStepid;
    this.schedulerName = schedulerName;
    this.schedulerCluster = schedulerCluster;
    this.genieId = genieId;
    this.netflixSparkVersion = netflixSparkVersion;
  }

  public String madId() {
    return madId;
  }

  public String schedulerWorkflowId() {
    return schedulerWorkflowId;
  }

  public String schedulerStepId() {
    return schedulerStepId;
  }

  public String genieId() {
    return genieId;
  }

  public String tableName() {
    return tableName;
  }

  public String schedulerName() {
    return schedulerName;
  }

  public String schedulerCluster() {
    return schedulerCluster;
  }

  public String netflixSparkVersion() {
    return netflixSparkVersion;
  }

  @Override
  public String toString() {
    return "madId='" + madId
        + ", schedulerWorkflowId='" + schedulerWorkflowId
        + ", schedulerStepId='" + schedulerStepId
        + ", schedulerName='" + schedulerName
        + ", schedulerCluster='" + schedulerCluster
        + ", genieId='" + genieId
        + ", tableName='" + tableName
        + ", netflixSparkVersion='" + netflixSparkVersion;
  }
}
