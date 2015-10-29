package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;

public class CustomerServiceStore extends AppDataSingleSchemaDimensionStoreHDHTUpdateWithList
{
  private static final long serialVersionUID = -7354676382869813092L;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Map<String, Long>>> satisfactionRatingOutputPort = new DefaultOutputPort<>();
  
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Map<String, Long>>> averageWaitTimeOutputPort = new DefaultOutputPort<>();
  
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> bandwidthUsageOutputPort = new DefaultOutputPort<>();
  
  //1 minute
  protected int timeBucket;
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    timeBucket = this.configurationSchema.getCustomTimeBucketRegistry().getTimeBucketId(new CustomTimeBucket(TimeBucket.MINUTE));
  }
  
  @Override
  protected void emitUpdates()
  {
    super.emitUpdates();
    emitUpdatesAverageFor("satisfaction", satisfactionRatingOutputPort);
    emitUpdatesAverageFor("wait", averageWaitTimeOutputPort);
  }
  
  final protected Map<String, Long> fieldValue = Maps.newHashMap();
  final protected List<Map<String, Long>> averageTuple = Lists.newArrayList(fieldValue);
  protected void emitUpdatesAverageFor(String fieldName, DefaultOutputPort<List<Map<String, Long>>> output)
  {
    if(!output.isConnected())
      return;

    long sum = 0;
    long count = 0;
    for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
      if(entry.getKey().getKey().getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET) != timeBucket)
        continue;
      int aggregatorId = entry.getKey().getAggregatorID();
      if(AggregatorIncrementalType.COUNT.ordinal() == aggregatorId)
        count = entry.getValue().getAggregates().getFieldLong(fieldName);
      if(AggregatorIncrementalType.SUM.ordinal() == aggregatorId)
        count = entry.getValue().getAggregates().getFieldLong(fieldName);
      if(sum != 0 && count != 0)
        break;
    }
    if(count != 0)
    {
      fieldValue.clear();
      fieldValue.put(fieldName, sum/count);
      output.emit(averageTuple);
    }
    
  }

  @Override
  protected DefaultOutputPort<List<Aggregate>> getOutputPort(int index, int aggregatorID, int dimensionDescriptorID)
  {
    return bandwidthUsageOutputPort;
  }
  

}
