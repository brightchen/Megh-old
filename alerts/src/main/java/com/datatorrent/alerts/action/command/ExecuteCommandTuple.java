package com.datatorrent.alerts.action.command;

import com.datatorrent.alerts.ActionTuple;

public class ExecuteCommandTuple extends ActionTuple {
  
  private String command;
  private String[] parameters;
  
  public ExecuteCommandTuple()
  {
    this.setAction(ActionType.EXECUTE_SCRIPT);
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public String[] getParameters() {
    return parameters;
  }

  public void setParameters(String[] parameters) {
    this.parameters = parameters;
  }

  
}
