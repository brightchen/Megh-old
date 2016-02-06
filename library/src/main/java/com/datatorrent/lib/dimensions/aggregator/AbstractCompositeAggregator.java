/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Set;

import com.google.common.collect.Sets;

import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema.DimensionsConversionContext;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;

/**
 * SimpleCompositAggregator is the aggregator which embed other aggregator
 *
 *
 * @param <T> the type of aggregator, could be OTFAggregator or IncrementalAggregator
 */
public abstract class AbstractCompositeAggregator<T> implements Aggregator<InputEvent, Aggregate>
{
  private static final long serialVersionUID = 661710563764433621L;

  
  /**
   * The embed aggregator could be OTFAggregator or IncrementalAggregator, 
   * but not another composite aggregator
   */
  protected T embedAggregator;
  protected String embedAggregatorName;
  
  protected DimensionsConversionContext dimensionsConversionContext;
  
  public AbstractCompositeAggregator<T> withEmbedAggregator(T embedAggregator)
  {
    this.setEmbedAggregator(embedAggregator);
    return this;
  }
  
  public T getEmbedAggregator()
  {
    return embedAggregator;
  }

  public void setEmbedAggregator(T embedAggregator)
  {
    this.embedAggregator = embedAggregator;
  }
  

  public DimensionsConversionContext getDimensionsConversionContext()
  {
    return dimensionsConversionContext;
  }

  public void setDimensionsConversionContext(DimensionsConversionContext dimensionsConversionContext)
  {
    this.dimensionsConversionContext = dimensionsConversionContext;
  }

  public AbstractCompositeAggregator<T> withDimensionsConversionContext(DimensionsConversionContext dimensionsConversionContext)
  {
    this.setDimensionsConversionContext(dimensionsConversionContext);
    return this;
  }
  
  public String getEmbedAggregatorName()
  {
    return embedAggregatorName;
  }

  public void setEmbedAggregatorName(String embedAggregatorName)
  {
    this.embedAggregatorName = embedAggregatorName;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dimensionsConversionContext == null) ? 0 : dimensionsConversionContext.hashCode());
    result = prime * result + ((embedAggregator == null) ? 0 : embedAggregator.hashCode());
    result = prime * result + ((embedAggregatorName == null) ? 0 : embedAggregatorName.hashCode());
    return result;
  }

//
//  @Override
//  public AbstractCompositeAggregator<T> clone()
//  {
//    return new AbstractCompositeAggregator().withEmbedAggregator(embedAggregator).withDimensionsConversionContext(dimensionsConversionContext);
//  }
}
