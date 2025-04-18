package com.netflix.iceberg;

import com.netflix.ksgateway.KsgProducer;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.events.CreateMADSnapshotEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateMADSnapshotListener {

  private static final String DEFAULT_KS_TOPIC = "spark_migration_iceberg_snapshots";
  private static final String TOPIC_CONFIG = "spark.keystone.madsnapshot.topic";
  private static final String KS_APP = "spark";
  private static final Logger LOG = LoggerFactory.getLogger(CreateMADSnapshotListener.class);

  private static volatile CreateMADSnapshotListener INSTANCE;

  @Nonnull
  private final KsgProducer producer;

  public static void initialize(Configuration conf) {
    try {
      CreateMADSnapshotListener listener = getInstance(conf);
      Listeners.register(listener::madSnapshotCreated, CreateMADSnapshotEvent.class);
    } catch (Exception e) {
      LOG.warn("Failed to initialize CreateMADSnapshotListener", e);
      throw new RuntimeException(e);
    }
  }

  private static CreateMADSnapshotListener getInstance(Configuration conf) {
    if (INSTANCE == null) {
      synchronized (CreateMADSnapshotListener.class) {
        if (INSTANCE == null) {
          INSTANCE = new CreateMADSnapshotListener(conf);
        }
      }
    }
    return INSTANCE;
  }

  private CreateMADSnapshotListener(Configuration conf) {
    this.producer = KsgProducer.builder()
        .stream(conf.get(TOPIC_CONFIG, DEFAULT_KS_TOPIC))
        .appName(KS_APP)
        .build();
  }

  @VisibleForTesting
  CreateMADSnapshotListener(KsgProducer producer) {
    this.producer = producer;
  }

  public void madSnapshotCreated(CreateMADSnapshotEvent event) {
    LOG.info("Sending CreateMADSnapshotEvent {} ", event);
    producer.send(generator -> {
      generator.writeStringField("workflow_id", event.workflowId());
      generator.writeStringField("genie_id", event.genieId());
      generator.writeStringField("step_id", event.stepId());
      generator.writeStringField("table_name", event.tableName());
      generator.writeNumberField("snapshotId", event.snapshotId());
      generator.writeStringField("mad_id", event.madId());
      generator.writeStringField("scheduler_name", event.schedulerName());
      generator.writeStringField("scheduler_cluster", event.schedulerCluster());
      generator.writeStringField("nflx_spark_version", event.netflixSparkVersion());
      generator.writeStringField("changed_partitions", event.changedPartitions());
      generator.writeStringField("added_data_files", event.addedFiles());
      generator.writeStringField("added_files_size", event.addedFileSize());
      generator.writeStringField("total_records", event.totalRecords());
      generator.writeStringField("total_delete_files", event.totalDeleteFiles());
      generator.writeStringField("changed_partition_count", event.changedPartitionCount());
    });
  }
}
