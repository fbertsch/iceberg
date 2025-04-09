package com.netflix.iceberg.metacat;

public enum TableType {
  DATA,
  FILES,
  ENTRIES,
  HISTORY,
  SNAPSHOTS,
  MANIFESTS,
  PARTITIONS,
  ALL_DATA_FILES,
  ALL_MANIFESTS,
  ALL_ENTRIES
}
