package com.netflix.iceberg.metacat;

import com.netflix.bdp.view.BaseMetastoreViews;
import com.netflix.bdp.view.ViewDefinition;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class ViewTest {

  private BaseMetastoreViews catalog;

  @Before
  public void setup() {
    Configuration conf = new Configuration();
    conf.addResource(ViewTest.class.getResourceAsStream("/hadoop/core-site.xml"));
    catalog = new MetacatViewCatalog(conf, "iceberg-client-integration-test");
  }

  @Test
  public void testRead() {
    ViewDefinition viewDefinition = catalog.loadDefinition("prodhive.common_view.common_view_presto");
    System.out.println(viewDefinition.schema());
  }
}
