package com.datatorrent.demos.dimensions.telecom;

import org.junit.Before;
import org.junit.Test;

import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.generate.EnrichedCDRHbaseInputOperator;

public class EnrichedCDRHbaseInputOperatorTester {
  
  @Before
  public void setUp()
  {
    EnrichedCDRHBaseConfig.instance.setHost("localhost");
  }
  
  @Test
  public void testInternal()
  {
    EnrichedCDRHbaseInputOperator operator = new EnrichedCDRHbaseInputOperator();
    operator.setup(null);
    operator.emitTuples();
  }

}
