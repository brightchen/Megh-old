package com.datatorrent.contrib.dimensions;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.datatorrent.lib.appdata.schemas.SchemaResultSerializer;

public class CompositeAggregatorSchemaQueryTester extends CompositeDimensionComputationTester
{
  @Override
  public void aggregationTest()
  {
  }
  
  @Test
  public void querySchemaTest()
  {
    //testCompositeAggregation();
    setupStore();
    querySchema();
  }
  
  public void querySchema()
  {
    SchemaResult schemaResult = store.getSchemaProcessor().getQueryExecutor().executeQuery(new SchemaQuery("1"), null, null);
    SchemaResultSerializer serializer = new SchemaResultSerializer();
    String serialized = serializer.serialize(schemaResult, null);
    LOG.info(serialized);
  }

  protected void doBeforeEndWindow(long windowId)
  {
    if(windowId != 1)
      return;
    
    querySchema();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryExecutorTest.class);
}
