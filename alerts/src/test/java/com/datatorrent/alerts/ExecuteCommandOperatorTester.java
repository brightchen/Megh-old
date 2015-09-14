package com.datatorrent.alerts;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.alerts.action.command.CommandOperator;
import com.datatorrent.alerts.action.command.ExecuteCommandTuple;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class ExecuteCommandOperatorTester {
  

  private static final Logger logger = LoggerFactory.getLogger(ExecuteCommandOperatorTester.class);

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
    
    

    TupleGenerator<ExecuteCommandTuple> generator = new TupleGenerator<ExecuteCommandTuple>();
    generator.setTuplesToEmit(getTuplesToEmit());
    
    CommandOperator testingOperator = new CommandOperator();

    dag.addOperator("generator", generator);
    dag.addOperator("testingOperator", testingOperator);

    dag.addStream("commandStream", generator.outputPort, testingOperator.input); // .setLocality(Locality.CONTAINER_LOCAL);

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
  
  public static final String[] commandLines = new String[]{ "ls -l -a", "lla", "dir", "whatever", "date", "echo 'aaa bbb'"};
  protected List<ExecuteCommandTuple> getTuplesToEmit()
  {
    
    List<ExecuteCommandTuple> tuples = new ArrayList<ExecuteCommandTuple>();
    for(int i=0; i<10; ++i)
    {
      ExecuteCommandTuple tuple = new ExecuteCommandTuple(commandLines[i%commandLines.length]);
      tuples.add(tuple);
    }
    
    return tuples;
  }

}
