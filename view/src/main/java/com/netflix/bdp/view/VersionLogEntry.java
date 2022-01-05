package com.netflix.bdp.view;

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class VersionLogEntry implements HistoryEntry {
    private final long timestampMillis;
    private final int versionId;

    VersionLogEntry(long timestampMillis, int versionId) {
        this.timestampMillis = timestampMillis;
        this.versionId = versionId;
    }

    public long timestampMillis() {
        return timestampMillis;
    }

    public int versionId() {
        return versionId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        VersionLogEntry that = (VersionLogEntry) other;
        return timestampMillis == that.timestampMillis && versionId == that.versionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, versionId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("timestampMillis", timestampMillis)
                .add("versionId", versionId)
                .toString();
    }
}
