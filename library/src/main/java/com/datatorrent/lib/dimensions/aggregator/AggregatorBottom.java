package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

public class AggregatorBottom<T> extends AbstractTopBottomAggregator<T>
{

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int computeHashCode(InputEvent object)
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(InputEvent o1, InputEvent o2)
  {
    // TODO Auto-generated method stub
    return false;
  }


}
