/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * The DefaultCompositeAggregatorFactory find the specific factory according to the aggregator type
 * and delegate to the specific factory.
 * 
 */
public class DefaultCompositeAggregatorFactory implements CompositeAggregatorFactory
{
  public static final DefaultCompositeAggregatorFactory defaultInst = new DefaultCompositeAggregatorFactory()
      .addFactory(AggregatorTopBottomType.TOPN.name(), TopBottomAggregatorFactory.defaultInstance)
      .addFactory(AggregatorTopBottomType.BOTTOMN.name(), TopBottomAggregatorFactory.defaultInstance);
  
  protected Map<String, CompositeAggregatorFactory> factoryRepository = Maps.newHashMap();
  
  //add this factory to create aggregator in a generic way by using reflection 
  protected CompositeAggregatorFactory genericCompositeAggregatorFactory;
  
  @Override
  public String getCompositeAggregatorName(String aggregatorType, String embedAggregatorName,
      Map<String, Object> properties)
  {
    return findSpecificFactory(aggregatorType).getCompositeAggregatorName(aggregatorType, embedAggregatorName, properties);
  }

  @Override
  public <T> AbstractCompositeAggregator<T> createCompositeAggregator(String aggregatorType, String embedAggregatorName, T embedAggregator,
      Map<String, Object> properties)
  {
    return findSpecificFactory(aggregatorType).createCompositeAggregator(aggregatorType, embedAggregatorName, embedAggregator, properties);
  }

  protected CompositeAggregatorFactory findSpecificFactory(String aggregatorType)
  {
    CompositeAggregatorFactory specificFactory = factoryRepository.get(aggregatorType);
    return specificFactory != null ? specificFactory : genericCompositeAggregatorFactory;
  }
  
  public DefaultCompositeAggregatorFactory addFactory(String aggregatorType, CompositeAggregatorFactory factory)
  {
    factoryRepository.put(aggregatorType, factory);
    return this;
  }
}
