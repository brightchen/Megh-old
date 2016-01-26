package com.datatorrent.lib.appdata.dimensions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class DimensionsComputationCompsiteAggregatorTest extends DimensionsComputationFlexibleSingleSchemaPOJOTest
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationCompsiteAggregatorTest.class);
  
  @Before
  public void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }
  
  @Test
  public void testBasicCase() throws Exception
  {
    final String configureFile = "adsGenericEventSimpleTopBottom.json";
    AdInfo ai = createTestAdInfoEvent1();
    AdInfo ai2 = createTestAdInfoEvent2();

    int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
    int dimensionsDescriptorID = 0;
    int aggregatorID = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.
                       getIncrementalAggregatorNameToID().
                       get(AggregatorIncrementalType.SUM.name());

    String eventSchema = SchemaUtils.jarResourceFileToString(configureFile);
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(eventSchema,
                                                               AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
    FieldsDescriptor keyFD = schema.getDimensionsDescriptorIDToKeyDescriptor().get(0);
    FieldsDescriptor valueFD = schema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().get(0).get(aggregatorID);

    GPOMutable keyGPO = new GPOMutable(keyFD);
    keyGPO.setField("publisher", "google");
    keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME,
                    TimeBucket.MINUTE.roundDown(300));
    keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET,
                    TimeBucket.MINUTE.ordinal());

    EventKey eventKey = new EventKey(0,
                                     schemaID,
                                     dimensionsDescriptorID,
                                     aggregatorID,
                                     keyGPO);

    GPOMutable valueGPO = new GPOMutable(valueFD);
    valueGPO.setField("clicks", ai.getClicks() + ai2.getClicks());
    valueGPO.setField("impressions", ai.getImpressions() + ai2.getImpressions());
    valueGPO.setField("revenue", ai.getRevenue() + ai2.getRevenue());
    valueGPO.setField("cost", ai.getCost() + ai2.getCost());

    Aggregate expectedAE = new Aggregate(eventKey, valueGPO);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = createDimensionsComputationOperator(configureFile);

    CollectorTestSink<Aggregate> sink = new CollectorTestSink<Aggregate>();
    TestUtils.setSink(dimensions.output, sink);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensionsClone =
    TestUtils.clone(new Kryo(), dimensions);

    dimensions.setup(null);

    dimensions.beginWindow(0L);
    dimensions.input.put(ai);
    dimensions.input.put(ai2);
    dimensions.endWindow();

    Assert.assertEquals("Expected only 1 tuple", 1, sink.collectedTuples.size());

    LOG.debug("{}", sink.collectedTuples.get(0).getKeys().toString());
    LOG.debug("{}", expectedAE.getKeys());
    LOG.debug("{}", sink.collectedTuples.get(0).getAggregates().toString());
    LOG.debug("{}", expectedAE.getAggregates().toString());

    Assert.assertEquals(expectedAE, sink.collectedTuples.get(0));
    Assert.assertEquals(expectedAE.getAggregates(), sink.collectedTuples.get(0).getAggregates());
  }
  

  @Test
  public void complexOutputTest()
  {
    AdInfo ai = createTestAdInfoEvent1();

    DimensionsComputationFlexibleSingleSchemaPOJO dcss = createDimensionsComputationOperator("adsGenericEventSimpleTopBottom.json");

    CollectorTestSink<Aggregate> sink = new CollectorTestSink<Aggregate>();
    TestUtils.setSink(dcss.output, sink);

    dcss.setup(null);
    dcss.beginWindow(0L);
    dcss.input.put(ai);
    dcss.endWindow();

    Assert.assertEquals(60, sink.collectedTuples.size());
  }
}
