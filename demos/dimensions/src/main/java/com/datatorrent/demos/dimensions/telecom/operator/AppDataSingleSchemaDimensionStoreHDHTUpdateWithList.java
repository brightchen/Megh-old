package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.MutablePair;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;

public abstract class AppDataSingleSchemaDimensionStoreHDHTUpdateWithList extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final transient Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHTUpdateWithList.class);
  
  private static final long serialVersionUID = -5870578159232945511L;

  /*
   * a list of pair of (aggregatorID, dimensionDescriptorID)
   * the DescriptorID is combination of ( dimensions and dimensions ), start with zero. see schema
   */
  protected List<MutablePair<Integer, Integer>> aggregatorsInfo;

  //private int aggregatorID;
  
  //the DescriptorID is combination of ( dimensions and dimensions ), start with zero. see schema
  //private int dimensionDescriptorID;

  protected transient List<Aggregate> updatingAggregates = Lists.newArrayList();
  
  @Override
  protected void emitUpdates()
  {
    super.emitUpdates();
    
    for(int index = 0; index < aggregatorsInfo.size(); ++index)
    {
      MutablePair<Integer, Integer> info = aggregatorsInfo.get(index);
      if(info == null)
        continue;
      
      int aggregatorID = info.left;
      int dimensionDescriptorID = info.right;
      DefaultOutputPort<List<Aggregate>> outputPort = getOutputPort(index++, aggregatorID, dimensionDescriptorID);
      if(outputPort == null || !outputPort.isConnected())
        continue;
      
      updatingAggregates.clear();
      
      for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
        if(aggregatorID == entry.getKey().getAggregatorID() && entry.getKey().getDimensionDescriptorID() == dimensionDescriptorID)
          updatingAggregates.add(entry.getValue());
      }

      outputPort.emit(updatingAggregates);
    }
  }

  protected abstract DefaultOutputPort<List<Aggregate>> getOutputPort(int index, int aggregatorID, int dimensionDescriptorID);
  
  public void addAggregatorsInfo(int aggregatorID, int dimensionDescriptorID)
  {
    if(aggregatorsInfo == null)
      aggregatorsInfo = Lists.newArrayList();
    aggregatorsInfo.add(new MutablePair<Integer, Integer>(aggregatorID, dimensionDescriptorID));
  }

  public List<MutablePair<Integer, Integer>> getAggregatorsInfo()
  {
    return aggregatorsInfo;
  }

  public void setAggregatorsInfo(List<MutablePair<Integer, Integer>> aggregatorsInfo)
  {
    this.aggregatorsInfo = aggregatorsInfo;
  }
  
  public void setAggregatorInfo(int index, int aggregatorID, int dimensionDescriptorID)
  {
    if(aggregatorsInfo == null)
      aggregatorsInfo = Lists.newArrayList();
    while(aggregatorsInfo.size() <= index)
      aggregatorsInfo.add(null);    //add the null item.
    
    aggregatorsInfo.set(index, new MutablePair<Integer, Integer>(aggregatorID, dimensionDescriptorID));
  }
  
}
