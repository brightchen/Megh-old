package com.datatorrent.demos.dimensions.telecom.generate;

import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public interface CustomerEnrichedInfoProvider
{
  public SingleRecord getRandomCustomerEnrichedInfo();
}
