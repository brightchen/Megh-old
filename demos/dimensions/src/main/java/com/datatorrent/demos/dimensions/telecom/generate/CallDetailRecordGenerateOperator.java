package com.datatorrent.demos.dimensions.telecom.generate;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class CallDetailRecordGenerateOperator implements InputOperator {
  public final transient DefaultOutputPort<byte[]> outputPort = new DefaultOutputPort<byte[]>();

  private int batchSize = 10;
  private CallDetailRecordCustomerInfoGenerator generator = new CallDetailRecordCustomerInfoGenerator();
  
   @Override
  public void beginWindow(long windowId) {
  }

  @Override
  public void endWindow() {
  }

  @Override
  public void setup(OperatorContext context) {}
  @Override
  public void teardown() {}

  @Override
  public void emitTuples() {
    for(int i=0; i<batchSize; ++i)
    {
      outputPort.emit(generator.next().toLine().getBytes());
    }
  }

}