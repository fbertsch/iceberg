/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.batch;

import com.netflix.bdp.s3.S3Committer;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.InternalRowWrapper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchPatternWrite implements Write, BatchWrite, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(BatchPatternWrite.class);
  private static final SortOrder[] NO_REQUIRED_ORDER = new SortOrder[0];
  private static final String HIVE_NULL = "__HIVE_DEFAULT_PARTITION__";
  private static final Joiner SLASH = Joiner.on('/');

  private final SparkSession spark;
  private final MetacatSparkTable table;
  private final String writeUUID;
  private final StructType writeSchema;
  private final boolean overwriteDynamic;
  private final Map<String, String> options;
  private final long batchId;
  private final int stageId;
  private final Configuration conf;
  private final Job job;
  private final OutputWriterFactory outputWriterFactory;
  private final JobContext jobContext;
  private final OutputCommitter jobCommitter;

  private Broadcast<SerializableConfiguration> confBroadcast = null;

  BatchPatternWrite(SparkSession spark, MetacatSparkTable table, String writeUUID, StructType schema,
                    boolean overwriteDynamic, Map<String, String> writeOptions) {
    this.spark = spark;
    this.table = table;
    this.writeUUID = writeUUID;
    this.writeSchema = schema;
    this.overwriteDynamic = overwriteDynamic;
    this.options = BatchUtil.buildTableOptions(table.options(), writeOptions);
    this.batchId = System.currentTimeMillis();
    this.stageId = writeUUID.hashCode() & Integer.MAX_VALUE; // get a unique ID until stage ID is available
    this.job = MapReduceUtil.newJob(buildConf(writeOptions), table.location());
    this.conf = job.getConfiguration();
    this.outputWriterFactory = table.format() != null ?
        table.format().prepareWrite(spark, job, ScalaUtil.asScala(options), writeSchema) :
        new HiveUtil.HiveOutputWriterFactory(confBroadcast(), table.info());
    this.jobContext = MapReduceUtil.newJobContext(conf, batchId, stageId);
    this.jobCommitter = MapReduceUtil.newJobCommitter(jobContext, table.spec().fields().size() >= 1);

    if (conf.getBoolean(S3Committer.FORCE_BATCH_REPLACE, S3Committer.DEFAULT_FORCE_BATCH_REPLACE) &&
        !overwriteDynamic) {
      LOG.warn("Detected forced overwrite: to suppress this warning, use INSERT OVERWRITE or SaveMode.Overwrite");
    }

    try {
      jobCommitter.setupJob(jobContext);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to initialize new committer for job: %s", jobContext.getJobID());
    }
  }

  private Broadcast<SerializableConfiguration> confBroadcast() {
    if (confBroadcast == null) {
      JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
      this.confBroadcast = sc.broadcast(new SerializableConfiguration(conf));
    }
    return confBroadcast;
  }

  private Configuration buildConf(Map<String, String> writeOptions) {
    Configuration conf = SparkTables.buildConf(spark, writeOptions);

    // add configuration for the batch committers
    conf.set("s3.multipart.committer.catalog", table.catalog());
    conf.set("s3.multipart.committer.database", table.database());
    conf.set("s3.multipart.committer.table", table.table());
    conf.set("s3.multipart.committer.batch-id", String.valueOf(batchId));
    conf.set("s3.multipart.committer.conflict-mode", overwriteDynamic ? "replace" : "append");

    // v1 writers set this and the committers use it to coordinate the Spark and S3 write UUIDs
    conf.set("spark.sql.sources.writeJobUUID", writeUUID);

    return conf;
  }

  @Override
  public String description() {
    String schema = Spark3Util.describe(SparkSchemaUtil.convert(writeSchema));
    String format = table.formatDescription();
    if (overwriteDynamic) {
      return String.format("BatchPatternReplace schema=%s, format=%s", schema, format);
    } else {
      return String.format("BatchPatternAppend schema=%s, format=%s", schema, format);
    }
  }

  @Override
  public BatchWrite toBatch() {
    return this;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    String genieId = conf.get("genie.job.id");
    boolean metricsEnabled = conf.getBoolean("spark.sql.partition.metrics.enabled", true);

    return new BatchPatternWriterFactory(
        outputWriterFactory, confBroadcast(), writeUUID, genieId, writeSchema, table.spec(), batchId, stageId,
        metricsEnabled);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (spark.sparkContext().conf().contains("spark.mad.id")
        && ((table.properties().containsKey("format") && table.properties().get("format").startsWith("hive"))
            || (!table.properties().containsKey("current-snapshot-id")))) {
		throw new RuntimeException("Attempting to write to a hive table with mad enabled");
    }

    try {
      jobCommitter.commitJob(jobContext);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to commit job: %s", jobContext.getJobID());
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    try {
      jobCommitter.abortJob(jobContext, JobStatus.State.FAILED);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to abort job: %s", jobContext.getJobID());
    }
  }

  @Override
  public Distribution requiredDistribution() {
    return Distributions.unspecified();
  }

  @Override
  public SortOrder[] requiredOrdering() {
    Distribution distribution = requiredDistribution();
    if (distribution instanceof OrderedDistribution) {
      return ((OrderedDistribution) distribution).ordering();
    } else if (Boolean.parseBoolean(SparkSession.active().conf().get("spark.sql.dsv2.inferOrdering", "true"))) {
      // infer a local ordering
      return SparkDistributionAndOrderingUtil.convert(Partitioning.sortOrderFor(table.spec()));
    }

    return NO_REQUIRED_ORDER;
  }

  private static class BatchPatternWriterFactory implements DataWriterFactory {
    private final OutputWriterFactory formatFactory;
    private final Broadcast<SerializableConfiguration> conf;
    private final String writeUUID;
    private final String genieId;
    private final StructType schema;
    private final PartitionSpec spec;
    private final long batchId;
    private final int stageId;
    private final boolean metricsEnabled;

    private BatchPatternWriterFactory(OutputWriterFactory formatFactory, Broadcast<SerializableConfiguration> conf,
                                      String writeUUID, String genieId, StructType schema, PartitionSpec spec,
                                      long batchId, int stageId, boolean metricsEnabled) {
      this.formatFactory = formatFactory;
      this.conf = conf;
      this.writeUUID = writeUUID;
      this.genieId = genieId;
      this.schema = schema;
      this.spec = spec;
      this.batchId = batchId;
      this.stageId = stageId;
      this.metricsEnabled = metricsEnabled;
    }

    OutputWriterFactory factory() {
      return formatFactory;
    }

    Configuration conf() {
      return conf.getValue().value();
    }

    String writeUUID() {
      return writeUUID;
    }

    String genieId() {
      return genieId;
    }

    StructType schema() {
      return schema;
    }

    PartitionSpec spec() {
      return spec;
    }

    boolean metricsEnabled() {
      return metricsEnabled;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      TaskAttemptContext taskContext = MapReduceUtil.newTaskContext(
          conf(), batchId, stageId, partitionId, taskId);
      if (spec.fields().size() < 1) {
        return new UnpartitionedWriter(this, taskContext);
      } else {
        return new PartitionedWriter(this, taskContext, schema);
      }
    }
  }

  private static abstract class BatchPatternWriter implements DataWriter<InternalRow> {
    private final BatchPatternWriterFactory config;
    private final TaskAttemptContext taskContext;
    private final FileOutputCommitter committer;
    private final String workPath;

    private BatchPatternWriter(BatchPatternWriterFactory config, TaskAttemptContext taskContext,
                               boolean isPartitioned) {
      this.config = config;
      this.taskContext = taskContext;
      this.committer = MapReduceUtil.newTaskCommitter(taskContext, isPartitioned);

      try {
        committer.setupTask(taskContext);

        // ensure that the working directory exists
        committer.getWorkPath().getFileSystem(config.conf()).mkdirs(committer.getWorkPath());

        this.workPath = committer.getWorkPath().toString();
      } catch (IOException e) {
        throw new RuntimeIOException(e,
            "Failed to initialize new committer for task: %s", taskContext.getTaskAttemptID());
      }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();
      committer.commitTask(taskContext);
      return new EmptyCommitMessage();
    }

    @Override
    public void abort() throws IOException {
      close();
      committer.abortTask(taskContext);
    }

    protected FileAppender<InternalRow> newWriter(String relativePath) {
      String location = SLASH.join(workPath, relativePath);
      FileAppender<InternalRow> appender = SparkTables.asAppender(
          config.factory().newInstance(location, config.schema(), taskContext));

      if (config.metricsEnabled()) {
        return MetacatMetricsAppender.wrap(appender, config.schema(), config.genieId(), location);
      } else {
        return appender;
      }
    }

    protected String filename() {
      int partitionId = taskContext.getTaskAttemptID().getTaskID().getId();
      return String.format("part-%05d-%s%s",
          partitionId, config.writeUUID(), config.factory().getFileExtension(taskContext));
    }
  }

  private static class UnpartitionedWriter extends BatchPatternWriter {
    private final FileAppender<InternalRow> currentWriter;

    UnpartitionedWriter(BatchPatternWriterFactory config, TaskAttemptContext taskContext) {
      super(config, taskContext, false);
      this.currentWriter = newWriter(filename());
    }

    @Override
    public void write(InternalRow row) {
      currentWriter.add(row);
    }

    @Override
    public void close() throws IOException {
      currentWriter.close();
    }
  }

  private static class PartitionedWriter extends BatchPatternWriter {
    private final HivePartitionKey key;
    private final InternalRowWrapper wrapper;
    private final Set<HivePartitionKey> completedPartitions = Sets.newHashSet();
    private HivePartitionKey currentKey = null;
    private FileAppender<InternalRow> currentWriter = null;

    PartitionedWriter(BatchPatternWriterFactory config, TaskAttemptContext taskContext, StructType schema) {
      super(config, taskContext, true);
      Schema rowSchema = SparkSchemaUtil.convert(schema);
      this.wrapper = new InternalRowWrapper(schema);
      this.key = new HivePartitionKey(config.spec(), rowSchema);
    }

    @Override
    public void write(InternalRow row) {
      key.partition(wrapper.wrap(row));

      if (!key.equals(currentKey)) {
        if (currentKey != null) {
          completedPartitions.add(currentKey);
        }
        closeCurrent();

        if (completedPartitions.contains(key)) {
          // if rows are not correctly grouped, detect and fail the write
          HivePartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
          LOG.warn("Duplicate key: {} == {}", existingKey, key);
          throw new IllegalStateException("Already closed files for partition: " + key.toPath());
        }

        this.currentKey = key.copy();
        this.currentWriter = newWriter(SLASH.join(currentKey.toPath(), filename()));
      }

      currentWriter.add(row);
    }

    @Override
    public void close() {
      closeCurrent();
    }

    private void closeCurrent() {
      if (currentWriter != null) {
        try {
          currentWriter.close();
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
      this.currentWriter = null;
      this.currentKey = null;
    }
  }

  private static class HivePartitionKey extends PartitionKey {
    public HivePartitionKey(PartitionSpec spec, Schema rowSchema) {
      super(spec, rowSchema);
    }

    protected HivePartitionKey(HivePartitionKey toCopy) {
      super(toCopy);
    }

    @Override
    public HivePartitionKey copy() {
      return new HivePartitionKey(this);
    }

    @Override
    public String toPath() {
      PartitionSpec spec = spec();
      StringBuilder sb = new StringBuilder();
      Class<?>[] javaClasses = spec.javaClasses();
      for (int i = 0; i < javaClasses.length; i += 1) {
        PartitionField field = spec.fields().get(i);
        if (i > 0) {
          sb.append("/");
        }
        // Use Spark to correctly escape partition paths for Hive
        if (get(i, Object.class) != null) {
          sb.append(ExternalCatalogUtils.getPartitionPathString(
              field.name(), field.transform().toHumanString(typedGet(i, javaClasses[i]))));
        } else {
          sb.append(ExternalCatalogUtils.getPartitionPathString(field.name(), HIVE_NULL));
        }
      }

      return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private <T> T typedGet(int pos, Class<?> javaClass) {
      return get(pos, (Class<T>) javaClass);
    }
  }

  private static class EmptyCommitMessage implements WriterCommitMessage {
  }
}
