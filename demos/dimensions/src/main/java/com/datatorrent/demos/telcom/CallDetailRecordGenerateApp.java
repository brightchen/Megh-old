package com.datatorrent.demos.telcom;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.telcom.generate.CallDetailRecordGenerateOperator;

@ApplicationAnnotation(name="CallDetailRecordGenerateApp")
public class CallDetailRecordGenerateApp implements StreamingApplication {
  protected String filePath;
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    CallDetailRecordGenerateOperator generator = new CallDetailRecordGenerateOperator();
    dag.addOperator("CDR-Generator", generator);
    
    HdfsBytesOutputOperator writer = new HdfsBytesOutputOperator();
    writer.setFilePath(filePath);
    dag.addOperator("CDR-Writer", writer);
    
    dag.addStream("CDR-Stream", generator.outputPort, writer.input);
  }

}
