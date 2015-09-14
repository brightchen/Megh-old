package com.datatorrent.alerts;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.alerts.action.email.EmailNotificationOperator;
import com.datatorrent.alerts.action.email.EmailNotificationTuple;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.util.TupleGenerateCacheOperator;

public class EmailNotificationOperatorTester {

  private static final Logger logger = LoggerFactory.getLogger(EmailNotificationOperatorTester.class);

  @Test
  public void test() throws Exception {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf) {
      }
    };
    
    

    TupleGenerator<EmailNotificationTuple> generator = new TupleGenerator<EmailNotificationTuple>();
    generator.setTuplesToEmit(getTuplesToEmit());
    
    EmailNotificationOperator testingOperator = new EmailNotificationOperator();

    dag.addOperator("generator", generator);
    dag.addOperator("testingOperator", testingOperator);

    dag.addStream("emailNotificationStream", generator.outputPort, testingOperator.input); // .setLocality(Locality.CONTAINER_LOCAL);

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    Thread.yield();
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
    }
    
  }
  
  protected List<EmailNotificationTuple> getTuplesToEmit()
  {
    EmailNotificationTuple tuple = new EmailNotificationTuple();
    List<EmailNotificationTuple> tuples = new ArrayList<EmailNotificationTuple>();
    for(int i=0; i<100; ++i)
    {
      tuples.add(tuple);
    }
    
    return tuples;
  }

}