package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;

public class CustomerServiceStore extends AppDataSingleSchemaDimensionStoreHDHTUpdateWithList
{
  private static final long serialVersionUID = -7354676382869813092L;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> satisfactionRatingOutputPort = new DefaultOutputPort<>();
  
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> averageWaitTimeOutputPort = new DefaultOutputPort<>();
  
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> bandwidthUsageOutputPort = new DefaultOutputPort<>();
  
  @Override
  protected DefaultOutputPort<List<Aggregate>> getOutputPort(int index, int aggregatorID, int dimensionDescriptorID)
  {
    if(index == 0)
      return satisfactionRatingOutputPort;
    if(index == 1)
      return averageWaitTimeOutputPort;
    if(index == 2)
      return bandwidthUsageOutputPort;
    
    throw new RuntimeException("Invalid index " + index);
  }
  
  public void setAggregatorInfoForSatisfactionRating(int aggregatorID, int dimensionDescriptorID)
  {
    setAggregatorInfo(0, aggregatorID, dimensionDescriptorID);
  }
  public void setAggregatorInfoForAverageWaitTime(int aggregatorID, int dimensionDescriptorID)
  {
    setAggregatorInfo(1, aggregatorID, dimensionDescriptorID);
  }
  public void setAggregatorInfoForBandwidthUsage(int aggregatorID, int dimensionDescriptorID)
  {
    setAggregatorInfo(2, aggregatorID, dimensionDescriptorID);
  }


}
