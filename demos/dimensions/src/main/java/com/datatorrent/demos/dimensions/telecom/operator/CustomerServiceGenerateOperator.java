package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.telecom.generate.CustomerServiceRandomGenerator;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;

public class CustomerServiceGenerateOperator implements InputOperator {
  public final transient DefaultOutputPort<CustomerService> outputPort = new DefaultOutputPort<CustomerService>();

  private int batchSize = 10;
  private CustomerServiceRandomGenerator generator = new CustomerServiceRandomGenerator();
  
  @Override
  public void beginWindow(long windowId) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void endWindow() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setup(OperatorContext context) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void teardown() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void emitTuples() {
    for(int i=0; i<batchSize; ++i)
    {
      outputPort.emit(generator.next());
    }    
  }

}
