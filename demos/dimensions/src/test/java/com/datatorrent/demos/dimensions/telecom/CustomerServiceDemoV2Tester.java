package com.datatorrent.demos.dimensions.telecom;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.app.CustomerServiceDemoV2;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerServiceHBaseConf;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

public class CustomerServiceDemoV2Tester extends CustomerServiceDemoV2{
  private static final Logger logger = LoggerFactory.getLogger(CustomerServiceDemoV2Tester.class);
  
  @Before
  public void setUp()
  {
    CustomerServiceHBaseConf.instance.setHost("localhost");
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
  

  @Override
  protected PubSubWebSocketAppDataQuery createAppDataQuery()
  {
    PubSubWebSocketAppDataQuery query = new PubSubWebSocketAppDataQuery();
    query.setTopic("telecomdemo-query");
    try
    {
      query.setUri(new URI("ws://localhost:9090/pubsub"));
    }
    catch(URISyntaxException uriE)
    {
      throw new RuntimeException(uriE);
    }
    
    return query;
  }
  
  @Override
  protected PubSubWebSocketAppDataResult createAppDataResult()
  {
    PubSubWebSocketAppDataResult wsOut = new PubSubWebSocketAppDataResult();
    wsOut.setTopic("telecomdemo-result");
    try
    {
      wsOut.setUri(new URI("ws://localhost:9090/pubsub"));
    }
    catch(URISyntaxException uriE)
    {
      throw new RuntimeException(uriE);
    }
    return wsOut;
  }
}