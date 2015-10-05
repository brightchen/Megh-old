package com.datatorrent.demos.dimensions.telecom;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.dimensions.telecom.model.CallDetailRecord;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;

public class CDREnrichOperator extends BaseOperator {
  
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<String> stringInputPort = new DefaultInputPort<String>()
  {
    @Override
    public void process(String t)
    {
      processTuple(t);
    }
  };
  
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<CallDetailRecord> cdrInputPort = new DefaultInputPort<CallDetailRecord>()
  {
    @Override
    public void process(CallDetailRecord t)
    {
      processTuple(t);
    }
  };
  public final transient DefaultOutputPort<EnrichedCDR> outputPort = new DefaultOutputPort<EnrichedCDR>();
  
  public void processTuple(String tuple)
  {
    EnrichedCDR enriched = EnrichedCDR.fromCallDetailRecord(tuple);
    outputPort.emit(enriched);
  }
  
  public void processTuple(CallDetailRecord tuple)
  {
    EnrichedCDR enriched = EnrichedCDR.fromCallDetailRecord(tuple);
    outputPort.emit(enriched);
  }
}
