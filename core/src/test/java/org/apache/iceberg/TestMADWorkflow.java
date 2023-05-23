package org.apache.iceberg;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestMADWorkflow extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1};
  }

  public TestMADWorkflow(int formatVersion) {
    super(formatVersion);
  }

  private static final String SCHEDULER_WORKFLOW_ID = "mad_workflow";
  private static final String SCHEDULER_STEP_ID = "step_id";
  private static final String MAD_ID = "mad_id";
  private static final String GENIE_ID = "genie_id";
  private static final String SCHEDULER_NAME = "maestro";
  private static final String SCHEDULER_CLUSTER = "prod";
  private static final String SPARK_VERSION = "3.3.2-nflx-245";

  private MadEventDetails madEventDetails(String tableName) {
    return new MadEventDetails(
        tableName,
        MAD_ID,
        SCHEDULER_WORKFLOW_ID,
        SCHEDULER_STEP_ID,
        SCHEDULER_NAME,
        SCHEDULER_CLUSTER,
        GENIE_ID,
        SPARK_VERSION);
  }

  @Test
  public void testStagingBehavior() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageForMAD(madEventDetails(table.name())).commit();

    // the overwrite should only be staged
    validateTableFiles(table, FILE_A);
  }

  @Test
  public void testMadIdInSnapshotSummary() {
    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    table.newAppend()
        .appendFile(FILE_B)
        .set(SnapshotSummary.STAGED_MAD_ID_PROP, "123456789")
        .stageForMAD(madEventDetails(table.name()))
        .commit();
    base = readMetadata();

    Snapshot madSnapshot = base.snapshots().get(1);

    Assert.assertEquals("Metadata should have both snapshots", 2, base.snapshots().size());
    Assert.assertEquals(
        "Current snapshot should be first commit's snapshot",
        firstSnapshotId,
        base.currentSnapshot().snapshotId());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1, base.snapshotLog().size());
    Assert.assertEquals(
        "Snapshot should have mad id in summary",
        "123456789",
        madSnapshot.summary().get(SnapshotSummary.STAGED_MAD_ID_PROP));
  }
}
