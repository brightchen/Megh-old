/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;

public abstract class AbstractTopBottomAggregator<T> extends AbstractCompositeAggregator<T> implements Aggregator<InputEvent, Aggregate>
{
  public static final String PROP_COUNT = "count";
  protected int count;
  protected SortedSet<String> subCombinations = Sets.newTreeSet();
  
  public AbstractTopBottomAggregator<T> withEmbedAggregator(T embedAggregator)
  {
    this.setEmbedAggregator(embedAggregator);
    return this;
  }
  public AbstractTopBottomAggregator<T> withEmbedAggregatorName(String embedAggregatorName)
  {
    this.setEmbedAggregatorName(embedAggregatorName);
    return this;
  }
  public AbstractTopBottomAggregator<T> withSubCombinations(String[] subCombinations)
  {
    this.setSubCombinations(subCombinations);
    return this;
  }
  
  public AbstractTopBottomAggregator<T> withCount(int count)
  {
    this.setCount(count);
    return this;
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }
  
  public void setSubCombinations(Set<String> subCombinations)
  {
    this.subCombinations.clear();
    this.subCombinations.addAll(subCombinations);
  }
  public void setSubCombinations(String[] subCombinations)
  {
    setSubCombinations(Sets.newHashSet(subCombinations));
  }
  public Set<String> getSubCombinations()
  {
    return subCombinations;
  }

  
  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    aggregateAggs(destAggs, srcAggs);
  }

  public void aggregateAggs(GPOMutable destAggs, GPOMutable srcAggs)
  {
    {
      byte[] destByte = destAggs.getFieldsByte();
      if (destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();

        for (int index = 0;
            index < destByte.length;
            index++) {
          destByte[index] += srcByte[index];
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if (destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();

        for (int index = 0;
            index < destShort.length;
            index++) {
          destShort[index] += srcShort[index];
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if (destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();

        for (int index = 0;
            index < destInteger.length;
            index++) {
          destInteger[index] += srcInteger[index];
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if (destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();

        for (int index = 0;
            index < destLong.length;
            index++) {
          destLong[index] += srcLong[index];
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if (destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();

        for (int index = 0;
            index < destFloat.length;
            index++) {
          destFloat[index] += srcFloat[index];
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if (destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();

        for (int index = 0;
            index < destDouble.length;
            index++) {
          destDouble[index] += srcDouble[index];
        }
      }
    }
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    aggregateInput(destAggs, srcAggs);
  }

  public void aggregateInput(GPOMutable destAggs, GPOMutable srcAggs)
  {
    int[] srcIndices = this.dimensionsConversionContext.indexSubsetAggregates.fieldsByteIndexSubset;
    
    {
      byte[] destByte = destAggs.getFieldsByte();
      if (destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();
        
        for (int index = 0;
            index < destByte.length;
            index++) {
          destByte[index] += srcByte[srcIndices[index]];
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if (destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();
        for (int index = 0;
            index < destShort.length;
            index++) {
          destShort[index] += srcShort[srcIndices[index]];
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if (destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();
        for (int index = 0;
            index < destInteger.length;
            index++) {
          destInteger[index] += srcInteger[srcIndices[index]];
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if (destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();
        for (int index = 0;
            index < destLong.length;
            index++) {
          destLong[index] += srcLong[srcIndices[index]];
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if (destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();
        for (int index = 0;
            index < destFloat.length;
            index++) {
          destFloat[index] += srcFloat[srcIndices[index]];
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if (destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();
        for (int index = 0;
            index < destDouble.length;
            index++) {
          destDouble[index] += srcDouble[srcIndices[index]];
        }
      }
    }
  }

  @Override
  public int hashCode()
  {
    return (embedAggregatorName.hashCode()*31 + count)*31 + subCombinations.hashCode();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    
    AbstractTopBottomAggregator other = (AbstractTopBottomAggregator)obj;
//    if (embedAggregator != other.embedAggregator
//        && (embedAggregator == null || !embedAggregator.equals(other.embedAggregator)))
//      return false;
    if (embedAggregatorName != other.embedAggregatorName
        && (embedAggregatorName == null || !embedAggregatorName.equals(other.embedAggregatorName)))
      return false;
    if (count != other.count)
      return false;
    if (subCombinations != other.subCombinations
        && (subCombinations == null || !subCombinations.equals(other.subCombinations)))
      return false;
    
    return true;
  }
  
  

  @Override
  public void aggregate(Aggregate resultAggregate, Set<EventKey> inputEventKeys,
      Map<EventKey, Aggregate> inputAggregatesRepo)
  {
    // TODO Auto-generated method stub
    
  }
}
