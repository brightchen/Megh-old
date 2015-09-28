package com.datatorrent.demos.telcom;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.telcom.generate.CallDetailRecordGenerateOperator;

/**
 * 
 * This application read Call Detail Record from files, enrich the CDR records
 * and save to the Hive
 *   - This application will remove the oldest files but keep the files not less than 120.
 * 
 * @author bright
 *
 */

@ApplicationAnnotation(name = "EnrichCDRApp")
public class EnrichCDRApp implements StreamingApplication {
  protected String filePath;

  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    HdfsStringInputOperator reader = new HdfsStringInputOperator();
    dag.addOperator("CDR-Reader", reader);

    CDREnrichOperator enrichOperator = new CDREnrichOperator();
    dag.addOperator("CDR-Enrich", enrichOperator);
    
    
    //CDRHiveOutputOperator outputOperator = new CDRHiveOutputOperator();
    
    //streams
    dag.addStream("CDR-Stream", reader.output, enrichOperator.inputPort);
  }

}
