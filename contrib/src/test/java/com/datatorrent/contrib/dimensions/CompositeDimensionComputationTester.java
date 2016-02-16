package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.StoreFSTestWatcher;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.util.TestUtils.TestInfo;

public class CompositeDimensionComputationTester
{
  @Rule
  public TestInfo testMeta = new StoreFSTestWatcher();
  
  protected final String configureFile = "compositeDimensionComputationSchema.json";
  
  protected final String publisher = "google";
  protected final String advertiser = "safeway";
  protected DimensionalConfigurationSchema eventSchema;
  
  
  @Test
  public void aggregationTest()
  {

    final String[] locations = {"CA", "WA", "ON", "BC"};
    final Map<String, Long> locationToImpressions = Maps.newHashMap();
    final Map<String, Double> locationToCost = Maps.newHashMap();
    long impression = 1;
    double cost = 100;
    for(String location : locations)
    {
      locationToImpressions.put(location, impression++);
      locationToCost.put(location, cost+1);
    }


    String eventSchemaString = SchemaUtils.jarResourceFileToString(configureFile);

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    eventSchema = store.configurationSchema;


    List<Aggregate> aggregates = Lists.newArrayList();
    for(String location : locationToImpressions.keySet())
    {
      aggregates.add(createEvent(AggregatorIncrementalType.SUM, location, locationToImpressions.get(location), locationToCost.get(location)));
      aggregates.add(createEvent(AggregatorIncrementalType.COUNT, location, locationToImpressions.get(location), locationToCost.get(location)));
    }
    long windowId = 1L;
    store.beginWindow(windowId);
    for(Aggregate aggregate : aggregates)
    {
      store.input.put(aggregate);
    }
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    windowId++;

    store.beginWindow(windowId);
    for(Aggregate aggregate : aggregates)
    {
      store.input.put(aggregate);
    }
 
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);

    store.teardown();
  }
  
  public Aggregate createEvent(
      AggregatorIncrementalType aggregatorType,
      String location,
      long impressions,
      double cost)
  {
    return createEvent(eventSchema,
                     aggregatorType,
                     publisher,
                     location,
                     60000L,
                     TimeBucket.MINUTE,
                     impressions,
                     cost);
  }

  public static Aggregate createEvent(DimensionalConfigurationSchema eventSchema,
                                      AggregatorIncrementalType aggregatorType,
                                      String publisher,
                                      String location,
                                      long timestamp,
                                      TimeBucket timeBucket,
                                      long impressions,
                                      double cost)
  {
    int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
    
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(aggregatorType.name());

    int dimensionDescriptorID = 2;
    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("location", location);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    EventKey eventKey = new EventKey(schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID,
                                     key);

    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);
    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);

    //Aggregate Event
    return new Aggregate(eventKey,
                              value);
  }
}
