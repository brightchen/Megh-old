package com.datatorrent.lib.dimensions.aggregator;

public class AggregatorBottom<T> extends AbstractTopBottomAggregator<T>
{
  @Override
  protected boolean shouldReplaceResultElement(int resultCompareToInput)
  {
    return resultCompareToInput < 0;
  }
}
