/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.StoreFSTestWatcher;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.QueryManagerAsynchronous;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
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
  protected final String FN_location = "location";
  protected final String FN_publisher = "publisher";
  protected final String VN_impressions = "impressions";
  protected final String VN_cost = "cost";
  
  //TODO: the value of SUM/COUNT seem not right after windowSize large than 2; windowSize = 3: sum/count multipule 4; 4 ==> 6.
  //the pattern like (n-1)*2, why?
  protected final int windowSize = 2;
  
  protected final String publisher = "google";
  //protected final String advertiser = "safeway";
  protected DimensionalConfigurationSchema eventSchema;
  protected TestStoreHDHT store;
  protected Set<EventKey> totalEventKeys = Sets.newHashSet();
  
  public static class TestStoreHDHT extends AppDataSingleSchemaDimensionStoreHDHT
  {
    private static final long serialVersionUID = -5241158406352270247L;

    public Map<EventKey, Aggregate> getCache()
    {
      return cache;
    }
    
    public Map<Integer, GPOMutable> getCompositeAggregteCache()
    {
      return compositeAggregteCache;
    }
    
    public QueryManagerAsynchronous<SchemaQuery, Void, Void, SchemaResult> getSchemaProcessor()
    {
      return schemaProcessor;
    }
  }
  
  @Test
  public void aggregationTest()
  {
    testCompositeAggregation();
  }
  
  public void setupStore()
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString(configureFile);

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    store = new TestStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);
  }
  
  protected void testCompositeAggregation()
  {
    final String[] locations = {"CA", "WA", "ON", "BC"};
    final Map<String, Long> locationToImpressions = Maps.newHashMap();
    final Map<String, Double> locationToCost = Maps.newHashMap();
    long impression = 50;
    double cost = 100;
    
    final Map<String, Double> costAverages = Maps.newHashMap();
    final Map<String, Double> costSums = Maps.newHashMap();
    final Map<String, Long> impressionSums = Maps.newHashMap();

    for(String location : locations)
    {
      costSums.put(location, cost*windowSize);
      impressionSums.put(location, impression*windowSize);
      costAverages.put(location, cost/2);
      
      locationToImpressions.put(location, impression++);
      locationToCost.put(location, cost++);
      
    }

    Map<String, Map<String, ?>> expectedAggregatorToValueFieldToValue = Maps.newHashMap();
    {
      //TOP
      {
        Map<String, Map<String, ?>> valueFieldToValue = Maps.newHashMap();
        valueFieldToValue.put(VN_cost, costSums);
        valueFieldToValue.put(VN_impressions, impressionSums);
        expectedAggregatorToValueFieldToValue.put("TOP", valueFieldToValue);
      }
      
      //BOTTOM
      {
        Map<String, Map<String, Double>> valueFieldToValue = Maps.newHashMap();
        valueFieldToValue.put(VN_cost, costAverages);
        expectedAggregatorToValueFieldToValue.put("BOTTOM", valueFieldToValue);
      }
    }
    
    setupStore();

    eventSchema = store.configurationSchema;

    
    List<Aggregate> aggregates = Lists.newArrayList();
    for(String location : locationToImpressions.keySet())
    {
      aggregates.add(createEvent(AggregatorIncrementalType.SUM, location, locationToImpressions.get(location), locationToCost.get(location)));
      //only cost has COUNT aggregator
      aggregates.add(createEvent(AggregatorIncrementalType.COUNT, location, null, 2L));
      
      Map<String, Number> valueKeyToValue = Maps.newHashMap();
      valueKeyToValue.put(VN_impressions, locationToImpressions.get(location));
      valueKeyToValue.put(VN_cost, locationToCost.get(location));
    }
    

    long windowId = 1L;
    for(int index = 0; index < windowSize; ++index)
    {
      store.beginWindow(windowId);
      for(Aggregate aggregate : aggregates)
      {
        store.input.put(aggregate);
      }
      
      doBeforeEndWindow(windowId);
      store.endWindow();
      
      totalEventKeys.addAll(store.getCache().keySet());
      
      store.checkpointed(windowId);
      store.committed(windowId);
      windowId++;
    }
    
    Map<String, Integer> nameToID = eventSchema.getAggregatorRegistry().getTopBottomAggregatorNameToID();
    int topId = nameToID.get("TOPN-SUM-10_location");
    int bottomId = nameToID.get("BOTTOMN-AVG-20_location");
    Map<EventKey, Aggregate> cache = store.getCache();
    Map<String, Map<String, Map<String,Object>>> aggregatorToValueFieldToValue = Maps.newHashMap();
    for(EventKey eventKey : totalEventKeys)
    {
      final GPOMutable values = store.fetchOrLoadAggregate(eventKey).getAggregates();
      
      //only care about the composite aggregator.
      //dimension 0/1 should only have composite aggregator
      int ddid = eventKey.getDimensionDescriptorID();
      if(ddid != 0 && ddid != 1)
      {
        //aggregator id should be only sum and count
        int aggregatorID = eventKey.getAggregatorID();
        Assert.assertTrue(aggregatorID == 0 || aggregatorID == 3);
        continue;
      }
      
      //composite field is only publisher
      List<String> fieldNames = eventKey.getKey().getFieldDescriptor().getFieldList();
      Set<String> fieldNameSet = Sets.newHashSet();
      fieldNameSet.addAll(fieldNames);
      fieldNameSet.remove("time");
      fieldNameSet.remove("timeBucket");
      Assert.assertTrue(fieldNameSet.size() == 1 && fieldNameSet.iterator().next().equals(FN_publisher));

      Map<String, Map<String,Object>> valueFieldToValue = Maps.newHashMap();
      valueFieldToValue.put(VN_cost, (Map<String,Object>)values.getFieldObject(VN_cost));
      //the AVG has one value cost, and TOP has value {impressions, cost}
      if(eventKey.getAggregatorID() == topId )
      {
        valueFieldToValue.put(VN_impressions, (Map<String,Object>)values.getFieldObject(VN_impressions));
        aggregatorToValueFieldToValue.put("TOP", valueFieldToValue);
      }
      else
      {
        aggregatorToValueFieldToValue.put("BOTTOM", valueFieldToValue);
      }
    }
    

    MapDifference diff = Maps.difference(expectedAggregatorToValueFieldToValue, aggregatorToValueFieldToValue);
    Assert.assertTrue(diff.toString(), diff.areEqual());
  }

  protected void doBeforeEndWindow(long windowId){}
  
  @After
  public void teardown()
  {
    if(store != null)
      store.teardown();
  }
  
  /**
   * The impressions and cost could be SUM or COUNT
   * @param aggregatorType
   * @param location
   * @param impressions
   * @param cost
   * @return
   */
  public Aggregate createEvent(
      AggregatorIncrementalType aggregatorType,
      String location,
      Long impressions,
      Object cost)
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
                                      Long impressions,
                                      Object cost)
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
    if(impressions != null)
      value.setField("impressions", impressions);
    if(cost != null)
    {
      if(AggregatorIncrementalType.COUNT.equals(aggregatorType))
        value.setField("cost", (Long)cost);
      else
        value.setField("cost", (Double)cost);
    }
    

    //Aggregate Event
    return new Aggregate(eventKey,
                              value);
  }
}