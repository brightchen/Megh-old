package com.datatorrent.lib.dimensions.aggregator;

public class AggregatorTop extends AbstractTopBottomAggregator
{
  @Override
  protected boolean shouldReplaceResultElement(int resultCompareToInput)
  {
    return resultCompareToInput > 0;
  }
}
