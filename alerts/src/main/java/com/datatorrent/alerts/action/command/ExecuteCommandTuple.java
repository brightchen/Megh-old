package com.datatorrent.alerts.action.command;

import com.datatorrent.alerts.ActionTuple;

public class ExecuteCommandTuple extends ActionTuple {
  
  private String command;
  private String[] parameters;
  
  public ExecuteCommandTuple(String commandLine)
  {
    if(commandLine == null || commandLine.isEmpty())
      throw new IllegalArgumentException("CommandLine can NOT null or empty.");
    String[] a = commandLine.split(" ");
    command = a[0];
    if(a.length>1)
    {
      parameters = new String[a.length-1];
      for(int i=0; i<a.length-1; ++i)
        parameters[i] = a[i+1];
    }
  }
  
  public ExecuteCommandTuple()
  {
    this.setAction(ActionType.EXECUTE_COMMAND);
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
