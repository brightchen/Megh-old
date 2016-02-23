package com.datatorrent.lib.dimensions.aggregator;

public class AggregatorTop<T> extends AbstractTopBottomAggregator<T>
{
  @Override
  protected boolean shouldReplaceResultElement(int resultCompareToInput)
  {
    return resultCompareToInput > 0;
  }
}
