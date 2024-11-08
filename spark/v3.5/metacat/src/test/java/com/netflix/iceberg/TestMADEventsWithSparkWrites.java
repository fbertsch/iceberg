package com.netflix.iceberg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.ksgateway.KsgProducer;
import com.netflix.ksgateway.KsgRequest;
import com.netflix.ksgateway.KsgResponse;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateMADSnapshotEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.NetflixConf;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.iceberg.SnapshotSummary.STAGED_MAD_ID_PROP;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class TestMADEventsWithSparkWrites {

  private static SparkSession spark = null;
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final String GENIE_ID = "saasa";
  private static final String MAD_ID = "asdasadsdasda";
  private static final String NETFLIX_SPARK_VERSION = "3.3-nflx-245";
  private static final String SCHEDULER_CLUSTER = "sandbox";
  private static final String SCHEDULER_NAME = "maestro";
  private static final String GENIE_TAGS = "scheduler.name:maestro,scheduler.cluster:sandbox";

  private static final String SCHEDULER_WORKFLOW_ID = "test-workflow";
  private static final String SCHEDULER_STEP_ID = "test-workflow-step";
  private static final String GENIE_GROUPING = SCHEDULER_WORKFLOW_ID + ":" + SCHEDULER_STEP_ID;
  private static final String CHANGED_PARTITION_COUNT = "3";
  private static final String ADDED_FILES = "3";
  // Spark 3.5 use parquet 1.13 and the file size is 1862 bytes
  private static final String ADDED_FILE_SIZE = "1862";
  private static final String TOTAL_RECORDS = "3";
  private static final String TOTAL_DELETE_FILES = "0";

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private KsgProducer producer;
  private CreateMADSnapshotListener listener;

  @BeforeClass
  public static void startSpark() {
    spark = SparkSession.builder().master("local[2]")
        .config(NetflixConf.GENIE_ID_CONF, GENIE_ID)
        .config(NetflixConf.GENIE_GROUPING_CONF, GENIE_GROUPING)
        .config(NetflixConf.MAD_ID_CONF, MAD_ID)
        .config(NetflixConf.GENIE_TAGS_CONF, GENIE_TAGS)
        .config(NetflixConf.NETFLIX_SPARK_VERSION_CONF, NETFLIX_SPARK_VERSION)
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    spark.stop();
    spark = null;
  }

  @Before
  public void init() {
    producer = Mockito.spy(
        KsgProducer.builder()
            .stream("test_stream")
            .appName("test_app_name")
            .hostname("test_hostname")
            .build()
    );
    listener = new CreateMADSnapshotListener(producer);
    Listeners.register(listener::madSnapshotCreated, CreateMADSnapshotEvent.class);
    Mockito.lenient().doReturn(KsgResponse.ok()).when(producer).send(Mockito.any(KsgRequest.class));
  }

  @Test
  public void testMADEventForWrites() throws Exception {
    File parent = temp.newFolder("mad_test_tables");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "10");
    Table table = tables.create(SCHEMA, spec, properties, location.toString());
    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(location.toString());
    assertMadEventSent(table, madSnapshotId(table));
  }

  private Long madSnapshotId(Table table) {
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.summary().containsKey(STAGED_MAD_ID_PROP)) {
        return snapshot.snapshotId();
      }
    }
    return null;
  }

  private void assertMadEventSent(Table table, Long expectedSnapshotId) throws Exception {
    ArgumentCaptor<KsgRequest> captor = ArgumentCaptor.forClass(KsgRequest.class);
    verify(producer).send(captor.capture());
    KsgRequest request = captor.getValue();
    assertEquals("test_hostname", request.getHostname());
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> payload = objectMapper.readValue(
        objectMapper.writeValueAsString(request.getEvents().get(0).getPayload()),
        new TypeReference<HashMap<String, Object>>() {}
    );
    assertEquals(GENIE_ID, payload.get("genie_id"));
    assertEquals(NETFLIX_SPARK_VERSION, payload.get("nflx_spark_version"));
    assertEquals(MAD_ID, payload.get("mad_id"));
    assertEquals(SCHEDULER_CLUSTER, payload.get("scheduler_cluster"));
    assertEquals(SCHEDULER_NAME, payload.get("scheduler_name"));
    assertEquals(SCHEDULER_WORKFLOW_ID, payload.get("workflow_id"));
    assertEquals(SCHEDULER_STEP_ID, payload.get("step_id"));
    assertEquals(table.name(), payload.get("table_name"));
    assertEquals(expectedSnapshotId, payload.get("snapshotId"));
    Set<String> changedPartitions = Sets.newHashSet("data=a", "data=b", "data=c");
    assertTrue(changedPartitions.containsAll(Arrays.asList(((String) payload.get("changed_partitions")).split(
        "::"))));
    assertEquals(ADDED_FILES, payload.get("added_data_files"));
    assertEquals(ADDED_FILE_SIZE, payload.get("added_files_size"));
    assertEquals(TOTAL_RECORDS, payload.get("total_records"));
    assertEquals(TOTAL_DELETE_FILES, payload.get("total_delete_files"));
    assertEquals(CHANGED_PARTITION_COUNT, payload.get("changed_partition_count"));
  }
}
