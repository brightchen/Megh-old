/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public class TopBottomAggregatorFactory extends AbstractCompositeAggregatorFactory
{
  public static final String PROPERTY_NAME_EMBEDED_AGGREGATOR = "embededAggregator";
  public static final String PROPERTY_NAME_COUNT = "count";
  public static final String PROPERTY_NAME_SUB_COMBINATIONS = "subCombinations";

  public static final TopBottomAggregatorFactory defaultInstance = new TopBottomAggregatorFactory();
  
  @Override
  public <T> AbstractTopBottomAggregator<T> createCompositeAggregator(String aggregatorType, String embedAggregatorName, T embedAggregator,
      Map<String, Object> properties)
  {
    return createTopBottomAggregator(aggregatorType, embedAggregatorName, embedAggregator, getCount(properties), getSubCombinations(properties));
  }
  
  public <T> AbstractTopBottomAggregator<T> createTopBottomAggregator(String aggregatorType, String embedAggregatorName, T embedAggregator, 
      int count, String[] subCombinations)
  {
    AbstractTopBottomAggregator<T> aggregator = null;
    if(AggregatorTopBottomType.TOPN == AggregatorTopBottomType.valueOf(aggregatorType))
    {
      aggregator = new AggregatorTop<T>();
    }
    if(AggregatorTopBottomType.BOTTOMN == AggregatorTopBottomType.valueOf(aggregatorType))
    {
      aggregator = new AggregatorBottom<T>();
    }
    if(aggregator == null)
    {
      throw new IllegalArgumentException("Invalid composite type: " + aggregatorType);
    }
    aggregator.setEmbedAggregator(embedAggregator);
    aggregator.setEmbedAggregatorName(embedAggregatorName);
    aggregator.setCount(count);
    aggregator.setSubCombinations(subCombinations);
    
    return aggregator;
  }

  protected int getCount(Map<String, Object> properties)
  {
    return Integer.valueOf((String)properties.get(PROPERTY_NAME_COUNT));
  }
  
  protected String[] getSubCombinations(Map<String, Object> properties)
  {
    return (String[])properties.get(PROPERTY_NAME_SUB_COMBINATIONS);
  }
  
  /**
   * The properties of TOP or BOTTOM are count and subCombinations.
   * count only have one value and subCombinations is a set of string, we can order combinations to simplify the name
   */
  @Override
  protected String getNamePartialForProperties(Map<String, Object> properties)
  {
    StringBuilder sb = new StringBuilder();
    String count = (String)properties.get(PROPERTY_NAME_COUNT);
    sb.append(count).append(PROPERTY_SEPERATOR);
    
    String[] subCombinations =  (String[])properties.get(PROPERTY_NAME_SUB_COMBINATIONS);
    Set<String> sortedSubCombinations = Sets.newTreeSet();
    for(String subCombination : subCombinations)
    {
      sortedSubCombinations.add(subCombination);
    }
    
    for(String subCombination : sortedSubCombinations)
    {
      sb.append(subCombination).append(PROPERTY_SEPERATOR);
    }

    //delete the last one (PROPERTY_SEPERATOR)
    return sb.deleteCharAt(sb.length()-1).toString();
  }
}
