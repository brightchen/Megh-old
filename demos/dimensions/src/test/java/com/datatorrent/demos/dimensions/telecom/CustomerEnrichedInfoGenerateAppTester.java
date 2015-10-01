package com.datatorrent.demos.dimensions.telecom;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.telcom.CustomerEnrichedInfoGenerateApp;

public class CustomerEnrichedInfoGenerateAppTester extends CustomerEnrichedInfoGenerateApp{
  @Before
  public void setUp()
  {
    this.hbaseConfig.setHost("localhost");
    this.hiveConfig.setHost("localhost");
  }
  
  @Test
  public void test() throws Exception {
    
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
