package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;
import java.util.Set;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;

public interface CompositeAggregator
{
  public int getSchemaID();
  public int getDimensionDescriptorID();
  public int getAggregatorID();
  
  public int getEmbedAggregatorDdId();
  public int getEmbedAggregatorID();
  public FieldsDescriptor getAggregateDescriptor();
  
  /**
  * @param compositeEventKey The composite event key, used to locate the target/dest aggregate
  * @param inputEventKeys The input(incremental) event keys, used to locate the input aggregates
  * @param inputEventKeyToAggregate The repository of input event key to aggregate. inputEventKeyToAggregate.keySet() should be a super set of inputEventKeys
  * */
  
  /**
   * 
   * @param resultAggregate the aggregate to put the result
   * @param inputEventKeys The input(incremental) event keys, used to locate the input aggregates
   * @param inputAggregatesRepo: the map of the EventKey to Aggregate keep the super set of aggregate required
   */
  public void aggregate(Aggregate resultAggregate, Set<EventKey> inputEventKeys, Map<EventKey, Aggregate> inputAggregatesRepo);
}
