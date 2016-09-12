package com.datatorrent.contrib.dimensions;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

@RunWith(value = Parameterized.class)
public class DimensionComputationFaultTolerantTest extends FaultTolerantTestApp
{
  public static final transient Logger logger = LoggerFactory.getLogger(DimensionComputationFaultTolerantTest.class);

  // name attribute is optional, provide an unique name for test
  // multiple parameters, uses Collection<Object[]>
  @Parameters(name = "{index}: applicationWindowCount: {0}; checkpointWindowCount: {1}")
  public static Collection<Integer[]> data()
  {
    return Arrays.asList(new Integer[][]{{2, 1}, {3, 2}, {2, 3}, {1, 2}});
  }

  public DimensionComputationFaultTolerantTest(int applicationWindowCount, int checkpointWindowCount)
  {
    super(applicationWindowCount, checkpointWindowCount);
  }

  @Test
  public void test()
  {
    try {
      runApplication();
    } catch (Exception e) {
      Assert.assertFalse(e.getMessage(), true);
    }
    logger.info("Test for applicationWindowCount: {}, checkpointWindowCount: {} done.==========================", applicationWindowCount, checkpointWindowCount);
  }

  public void runApplication() throws Exception
  {
    Configuration conf = new Configuration(false);

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    tupleCount = 0;
    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    while (tupleCount < tupleSize) {
      try {
        Thread.sleep(500);
      } catch (Exception e) {
        //ignore
      }
    }
    lc.shutdown();
    
    //the second setup would be failed if throw out exception
    Assert.assertTrue("setupTimes: " + setupTimes,  setupTimes == 2);
  }
}
