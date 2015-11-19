package com.datatorrent.demos.dimensions.telecom;

import org.junit.Test;

import com.datatorrent.demos.dimensions.telecom.generate.CallDetailRecordCustomerInfoGenerator;
import com.datatorrent.demos.dimensions.telecom.model.CallDetailRecord;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;

public class CDREnrichTester {
  @Test
  public void test() throws Exception {
    CallDetailRecordCustomerInfoGenerator generator = new CallDetailRecordCustomerInfoGenerator();
    for( int i=0; i<100; ++i )
    {
      CallDetailRecord cdr = generator.next();
      EnrichedCDR enriched = EnrichedCDR.fromCallDetailRecord(cdr.toLine());
      String line = enriched.toLine();
      System.out.println(line);
    }
 
  }
}
