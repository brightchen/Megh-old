package com.datatorrent.demos.dimensions.telecom;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.telcom.EnrichCDRApp;
import com.datatorrent.demos.telcom.conf.CDREnrichedRecordHBaseConfig;

public class EnrichCDRAppTester extends EnrichCDRApp{
private static final Logger logger = LoggerFactory.getLogger(EnrichCDRAppTester.class);
  
  @Test
  public void test() throws Exception {
    cdrDir = "CDR/";
    CDREnrichedRecordHBaseConfig.instance.setHost("localhost");
    
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf) {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    
    Thread.sleep(600000);

    lc.shutdown();
  }
}
