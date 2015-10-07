package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.telecom.generate.CallDetailRecordCustomerInfoGenerator;
import com.datatorrent.demos.dimensions.telecom.model.CallDetailRecord;

public class CallDetailRecordGenerateOperator implements InputOperator {
  public final transient DefaultOutputPort<byte[]> bytesOutputPort = new DefaultOutputPort<byte[]>();
  public final transient DefaultOutputPort<CallDetailRecord> cdrOutputPort = new DefaultOutputPort<CallDetailRecord>();

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
    if(bytesOutputPort.isConnected())
    {
      for(int i=0; i<batchSize; ++i)
      {
        bytesOutputPort.emit(generator.next().toLine().getBytes());
      }
    }
    if(cdrOutputPort.isConnected())
    {
      for(int i=0; i<batchSize; ++i)
      {
        cdrOutputPort.emit(generator.next());
      }
    }
  }

}