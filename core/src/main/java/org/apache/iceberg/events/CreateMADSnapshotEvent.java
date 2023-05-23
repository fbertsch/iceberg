package org.apache.iceberg.events;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.MadEventDetails;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;

public class CreateMADSnapshotEvent {
  private final MadEventDetails madEventDetails;
  private final long snapshotId;
  private final String changedPartitions;
  private final Map<String, String> madSnapshotSummary;

  public CreateMADSnapshotEvent(MadEventDetails madEventDetails, long snapshotId, Map<String, String> summary) {
    this.madEventDetails = madEventDetails;
    this.snapshotId = snapshotId;
    this.changedPartitions = changedPartitions(summary);
    this.madSnapshotSummary = madSnapshotSummary(summary);
  }

  public String madId() {
    return madEventDetails.madId();
  }

  public String workflowId() {
    return madEventDetails.schedulerWorkflowId();
  }

  public String stepId() {
    return madEventDetails.schedulerStepId();
  }

  public String genieId() {
    return madEventDetails.genieId();
  }

  public String tableName() {
    return madEventDetails.tableName();
  }

  public String schedulerName() {
    return madEventDetails.schedulerName();
  }

  public String schedulerCluster() {
    return madEventDetails.schedulerCluster();
  }

  public long snapshotId() {
    return snapshotId;
  }

  public String netflixSparkVersion() {
    return madEventDetails.netflixSparkVersion();
  }

  @Override
  public String toString() {
    return "snapshotId=" + snapshotId
        + ", " + madEventDetails;
  }

  public String changedPartitions() {
    return changedPartitions;
  }

  public String addedFiles() {
    return madSnapshotSummary.get(SnapshotSummary.ADDED_FILES_PROP);
  }

  public String addedFileSize() {
    return madSnapshotSummary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP);
  }

  public String totalRecords() {
    return madSnapshotSummary.get(SnapshotSummary.TOTAL_RECORDS_PROP);
  }

  public String totalDeleteFiles() {
    return madSnapshotSummary.get(SnapshotSummary.TOTAL_DELETE_FILES_PROP);
  }

  public String changedPartitionCount() {
    return madSnapshotSummary.get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
  }

  private static Map<String, String> madSnapshotSummary(Map<String, String> snapshotSummary) {
    Map<String, String> localMap = new HashMap<>();
    localMap.put(SnapshotSummary.ADDED_FILES_PROP, snapshotSummary.get(SnapshotSummary.ADDED_FILES_PROP));
    localMap.put(SnapshotSummary.ADDED_FILE_SIZE_PROP, snapshotSummary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP));
    localMap.put(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, snapshotSummary.get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP));
    localMap.put(SnapshotSummary.TOTAL_RECORDS_PROP, snapshotSummary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
    localMap.put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, snapshotSummary.get(SnapshotSummary.TOTAL_DELETE_FILES_PROP));
    return localMap;
  }

  private static String changedPartitions(Map<String, String> snapshotSummary) {
    Set<String> changedPartitions = snapshotSummary.keySet()
        .stream()
        .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
        .map(key -> key.substring(
            key.indexOf(SnapshotSummary.CHANGED_PARTITION_PREFIX) + SnapshotSummary.CHANGED_PARTITION_PREFIX.length()))
        .collect(Collectors.toSet());
    return Joiner.on("::").join(changedPartitions);
  }
}
