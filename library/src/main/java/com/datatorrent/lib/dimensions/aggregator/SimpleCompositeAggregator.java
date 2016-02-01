/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

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
public abstract class SimpleCompositeAggregator<T> implements Aggregator<InputEvent, Aggregate>
{
  /**
   * The embed aggregator could be OTFAggregator or IncrementalAggregator, 
   * but not another composite aggregator
   */
  protected T embededAggregator;
  protected DimensionsConversionContext dimensionsConversionContext;
  
  public SimpleCompositeAggregator<T> withEmbededAggregator(T embededAggregator)
  {
    this.setEmbededAggregator(embededAggregator);
    return this;
  }
  
  public T getEmbededAggregator()
  {
    return embededAggregator;
  }

  public void setEmbededAggregator(T embededAggregator)
  {
    this.embededAggregator = embededAggregator;
  }
  

  public DimensionsConversionContext getDimensionsConversionContext()
  {
    return dimensionsConversionContext;
  }

  public void setDimensionsConversionContext(DimensionsConversionContext dimensionsConversionContext)
  {
    this.dimensionsConversionContext = dimensionsConversionContext;
  }

  public SimpleCompositeAggregator<T> withDimensionsConversionContext(DimensionsConversionContext dimensionsConversionContext)
  {
    this.setDimensionsConversionContext(dimensionsConversionContext);
    return this;
  }

  @Override
  public SimpleCompositeAggregator<Object> clone()
  {
    return new SimpleCompositeAggregator<Object>().withEmbededAggregator(embededAggregator).withDimensionsConversionContext(dimensionsConversionContext);
  }
}
