package org.apache.iceberg.spark.actions;

import org.apache.iceberg.Snapshot;

import org.apache.iceberg.*;
import org.apache.iceberg.actions.NfCloneTable;
import org.apache.iceberg.actions.NfCloneTableActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NfCloneTableSparkAction extends BaseSparkAction<NfCloneTableSparkAction>
        implements NfCloneTable {
    private static final Logger LOG = LoggerFactory.getLogger(NfCloneTableSparkAction.class);

    private final String sourceTableName;
    private final String cloneTableName;
    private final boolean includeSnapshots;
    private final Map<String, String> additionalProperties;


    public NfCloneTableSparkAction(SparkSession spark, String sourceTableName, String cloneTableName, Map<String, String> additionalProperties, boolean includeSnapshots) {
        super(spark);
        this.sourceTableName = sourceTableName;
        this.cloneTableName = cloneTableName;
        this.additionalProperties = additionalProperties;
        this.includeSnapshots = includeSnapshots;
    }

    @Override
    protected NfCloneTableSparkAction self() {
        return this;
    }

    public NfCloneTable.Result execute() {
        LOG.info("Parsing sourceTable identifier {}", sourceTableName);
        TableIdentifier sourceTableIdentifier = TableIdentifier.parse(sourceTableName);

        LOG.info("Parsing cloneTable identifier {}", cloneTableName);
        TableIdentifier cloneTableIdentifier = TableIdentifier.parse(cloneTableName);
        Table cloneTable;

        CatalogPlugin catalogPlugin = spark().sessionState().catalogManager().currentCatalog();
        Catalog catalog = ((HasIcebergCatalog) catalogPlugin).icebergCatalog();

        LOG.info("Creating the clone table ({}) from ({})", cloneTableIdentifier, sourceTableIdentifier);
        cloneTable = catalog.cloneTable(sourceTableIdentifier, cloneTableIdentifier, additionalProperties, includeSnapshots);

        if (!includeSnapshots) {
            LOG.info("Table was cloned, but no snapshots were copied because includeSnapshots was set to true");
            return new NfCloneTableActionResult(0L);
        }

        Snapshot snapshot = cloneTable.currentSnapshot();

        long clonedDataFilesCount = Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
        LOG.info("Cloned data files count: {}", clonedDataFilesCount);
        return new NfCloneTableActionResult(clonedDataFilesCount);
    }

    @Override
    public NfCloneTableSparkAction tableProperties(Map<String, String> properties) {
        this.additionalProperties.putAll(properties);
        return this;
    }

    @Override
    public NfCloneTableSparkAction tableProperty(String property, String value) {
        additionalProperties.put(property, value);
        return this;
    }

}
