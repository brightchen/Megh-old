package com.datatorrent.alerts.action.command;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.alerts.ActionHandler;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * @displayName Alerts Command Operator
 * @category Alerts
 * @tags alerts, command
 * @since 3.1.0
 */
@Evolving
public class CommandOperator extends BaseOperator{
  private transient ActionHandler<ExecuteCommandTuple> actionHandler = new ExecuteCommandHandler();
  
  public CommandOperator(String filePath)
  {
    this.setAlertsCommandWhiteListConfFile(filePath);
  }
  
  public CommandOperator()
  {
  }
  
  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ExecuteCommandTuple> input = new DefaultInputPort<ExecuteCommandTuple>()
  {
    @Override
    public void process(ExecuteCommandTuple t)
    {
      processTuple(t);
    }

  };
  
  public void setAlertsCommandWhiteListConfFile(String filePath)
  {
    System.setProperty(DefaultWhiteListProvider.PROP_ALERTS_COMMAND_WHITELIST_CONF_FILE, filePath);
  }
  
  public void processTuple(ExecuteCommandTuple tuple)
  {
    actionHandler.handle(tuple);
  }
  

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }
  

  @Override
  public void teardown()
  {
  }
}
