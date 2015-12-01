package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCustomerService;

public class CustomerServiceEnrichOperator extends BaseOperator
{

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<CustomerService> inputPort = new DefaultInputPort<CustomerService>()
  {
    @Override
    public void process(CustomerService t)
    {
      processTuple(t);
    }
  };
  public final transient DefaultOutputPort<EnrichedCustomerService> outputPort = new DefaultOutputPort<EnrichedCustomerService>();

  public void processTuple(CustomerService tuple)
  {
    EnrichedCustomerService enriched = EnrichedCustomerService.fromCustomerService(tuple);
    outputPort.emit(enriched);
  }
}
