package com.datatorrent.alerts;

import java.util.List;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

public class TupleGenerator<T> implements InputOperator {
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  private int batchNum = 5;
  private List<T> tuplesToEmit;
  private int emittedCount = 0;

  
  public List<T> getTuplesToEmit() {
    return tuplesToEmit;
  }

  public void setTuplesToEmit(List<T> tuplesToEmit) {
    this.tuplesToEmit = tuplesToEmit;
  }

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
    if (emittedCount >= tuplesToEmit.size()) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {
      }
      return;
    }

    for (int i = 0; i < batchNum && emittedCount < tuplesToEmit.size(); ++i) {
      outputPort.emit(tuplesToEmit.get(emittedCount));
      emittedCount++;
    }

  }

}