package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;

public class AppDataSingleSchemaDimensionStoreHDHTUpdateWithList extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final transient Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHTUpdateWithList.class);
  
  private static final long serialVersionUID = -5870578159232945511L;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> updateWithList = new DefaultOutputPort<>();

  protected transient List<Aggregate> updatingAggregates = Lists.newArrayList();

  private int aggregatorID;
  
  //the DescriptorID is combination of ( dimensions and dimensions ), start with zero. see schema
  private int dimensionDescriptorID;
  
  @Override
  protected void emitUpdates()
  {
    super.emitUpdates();

    if (updateWithList.isConnected()) {
      updatingAggregates.clear();
      
      for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
        if(aggregatorID == entry.getKey().getAggregatorID() && entry.getKey().getDimensionDescriptorID() == dimensionDescriptorID)
          updatingAggregates.add(entry.getValue());
      }
      updateWithList.emit(updatingAggregates);
      logger.info("Cached {}, emit aggregtes {}", cache.size(), updatingAggregates.size());
    }
  }

  public int getAggregatorID()
  {
    return aggregatorID;
  }

  public void setAggregatorID(int aggregatorID)
  {
    this.aggregatorID = aggregatorID;
  }

  public int getDimensionDescriptorID()
  {
    return dimensionDescriptorID;
  }

  public void setDimensionDescriptorID(int dimensionDescriptorID)
  {
    this.dimensionDescriptorID = dimensionDescriptorID;
  }
  
  
}
