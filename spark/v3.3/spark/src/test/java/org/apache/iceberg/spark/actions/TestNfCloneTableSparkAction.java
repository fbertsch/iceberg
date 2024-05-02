package org.apache.iceberg.spark.actions;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.NfCloneTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.internal.SessionState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

public class TestNfCloneTableSparkAction {
    private SparkSession spark;
    private String sourceTable;
    private String destinationTable;
    private Map<String, String> properties;
    private boolean includeSnapshots;
    private Catalog catalog;
    private Table table;
    private CatalogManager catalogManager;
    // Cast the CatalogPlugin to HasIcebergCatalog and create a mock Catalog
    private HasIcebergCatalog hasIcebergCatalog;
    private SessionState sessionState;
    private SparkContext sparkContext;


    @Before
    public void setUp() {
        // Create a mock SparkSession
        spark = Mockito.mock(SparkSession.class);

        // Set up source and destination table names
        sourceTable = "source_table";
        destinationTable = "destination_table";
        includeSnapshots = false;

        // Set up properties
        properties = new HashMap<>();
        properties.put("property1", "value1");
        properties.put("property2", "value2");

        catalog = Mockito.mock(Catalog.class);
        table = Mockito.mock(Table.class);
        catalogManager = Mockito.mock(CatalogManager.class);
        hasIcebergCatalog = Mockito.mock(HasIcebergCatalog.class);
        sessionState = Mockito.mock(SessionState.class);
        sparkContext = Mockito.mock(SparkContext.class);

    }

    @Test
    public void testExecute() {
        when(catalog.cloneTable(any(TableIdentifier.class), any(TableIdentifier.class), anyMap(), anyBoolean())).thenReturn(table);
        when(hasIcebergCatalog.icebergCatalog()).thenReturn(catalog);
        when(spark.sessionState()).thenReturn(sessionState);
        when(spark.sparkContext()).thenReturn(sparkContext);
        when(sessionState.catalogManager()).thenReturn(catalogManager);
        // Returning instance of hasIcebergCatalog given that catalogPlugin should be Iceberg Catalog
        when(catalogManager.currentCatalog()).thenReturn(hasIcebergCatalog);
        includeSnapshots = true;

        NfCloneTableSparkAction action = new NfCloneTableSparkAction(spark, sourceTable, destinationTable, properties, includeSnapshots);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.summary()).thenReturn(ImmutableMap.of(SnapshotSummary.TOTAL_DATA_FILES_PROP, "100"));
        when(table.currentSnapshot()).thenReturn(snapshot);

        NfCloneTable.Result result = action.execute();
        assertEquals(Long.valueOf(100), Long.valueOf(result.clonedDataFilesCount()));
    }

    @Test
    public void testIncludeSnapshots() {
        when(catalog.cloneTable(any(TableIdentifier.class), any(TableIdentifier.class), anyMap(), anyBoolean())).thenReturn(table);
        when(hasIcebergCatalog.icebergCatalog()).thenReturn(catalog);
        when(spark.sessionState()).thenReturn(sessionState);
        when(spark.sparkContext()).thenReturn(sparkContext);
        when(sessionState.catalogManager()).thenReturn(catalogManager);
        // Returning instance of hasIcebergCatalog given that catalogPlugin should be Iceberg Catalog
        when(catalogManager.currentCatalog()).thenReturn(hasIcebergCatalog);
        includeSnapshots = false;

        NfCloneTableSparkAction action = new NfCloneTableSparkAction(spark, sourceTable, destinationTable, properties, includeSnapshots);

        NfCloneTable.Result result = action.execute();
        assertEquals(Long.valueOf(0), Long.valueOf(result.clonedDataFilesCount()));
    }
}
