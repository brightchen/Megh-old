package com.datatorrent.demos.dimensions.telecom;

import org.junit.Test;

import com.datatorrent.demos.telcom.generate.CallDetailRecordRandomGenerator;
import com.datatorrent.demos.telcom.model.CallDetailRecord;
import com.datatorrent.demos.telcom.model.EnrichedCDR;

public class CDREnrichTester {
  @Test
  public void test() throws Exception {
    CallDetailRecordRandomGenerator generator = new CallDetailRecordRandomGenerator();
    for( int i=0; i<100; ++i )
    {
      CallDetailRecord cdr = generator.next();
      EnrichedCDR enriched = EnrichedCDR.fromCallDetailRecord(cdr.toLine());
      String line = enriched.toLine();
      System.out.println(line);
    }
 
  }
}
