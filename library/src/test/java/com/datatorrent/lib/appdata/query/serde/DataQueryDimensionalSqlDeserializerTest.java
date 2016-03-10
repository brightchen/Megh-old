package com.datatorrent.lib.appdata.query.serde;

import java.util.HashSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaRegistrySingle;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.google.common.collect.Sets;

public class DataQueryDimensionalSqlDeserializerTest
{
  @Rule
  public DeserializerTestWatcher testMeta = new DeserializerTestWatcher();
  
  public static class DeserializerTestWatcher extends TestWatcher
  {
    private SchemaRegistrySingle schemaRegistry;

    @Override
    protected void starting(Description description)
    {
      DimensionalSchema schema = new DimensionalSchema(new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json"),
                                                                                          AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));
      schemaRegistry = new SchemaRegistrySingle();
      schemaRegistry.registerSchema(schema);
    }

    public SchemaRegistrySingle getSchemaRegistry()
    {
      return schemaRegistry;
    }
  }

  @BeforeClass
  public static void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  /**
   * The modification could affect json deserialize, test it
   * @throws Exception
   */
  @Test
  public void testJsonDeserialize() throws Exception
  {
    String json = SchemaUtils.jarResourceFileToString("dimensionalDataQuery.json");
    DataQueryDimensional dqd = deserializeDataQueryDimensional(json);
    validateDataQueryDimensional(dqd);
    Assert.assertEquals(10, dqd.getLatestNumBuckets());
  }
  
  protected DataQueryDimensional deserializeDataQueryDimensional(String queryString) throws Exception
  {
    MessageDeserializerManagementFactory queryDeserializerFactory = new MessageDeserializerManagementFactory(SchemaQuery.class, DataQueryDimensional.class);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, testMeta.getSchemaRegistry());
    
    return (DataQueryDimensional) queryDeserializerFactory.deserialize(queryString);
  }
  

  protected void validateDataQueryDimensional(DataQueryDimensional dataQueryDimensional)
  {
    Assert.assertEquals("1", dataQueryDimensional.getId());
    Assert.assertEquals(TimeBucket.MINUTE, dataQueryDimensional.getTimeBucket());
    Assert.assertEquals(true, dataQueryDimensional.getIncompleteResultOK());
    Assert.assertEquals(new HashSet<String>(), dataQueryDimensional.getKeyFields().getFields());
    Assert.assertEquals(Sets.newHashSet("tax", "sales", "discount"),
                        dataQueryDimensional.getFieldsAggregatable().getAggregatorToFields().get("SUM"));
    Assert.assertEquals(Sets.newHashSet("time", "channel", "region", "product"),
                        dataQueryDimensional.getFieldsAggregatable().getNonAggregatedFields().getFields());
  }
}
