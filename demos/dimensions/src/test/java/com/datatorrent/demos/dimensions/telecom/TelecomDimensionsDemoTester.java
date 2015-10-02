package com.datatorrent.demos.dimensions.telecom;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.TelecomDimensionsDemo;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;

public class TelecomDimensionsDemoTester extends TelecomDimensionsDemo{
private static final Logger logger = LoggerFactory.getLogger(TelecomDimensionsDemoTester.class);
  
  @Test
  public void test() throws Exception {
    EnrichedCDRHBaseConfig.instance.setHost("localhost");
    
    Configuration conf = new Configuration(false);
    conf.set(TelecomDimensionsDemo.PROP_STORE_PATH, "~/temp");
    
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    

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
