package com.datatorrent.alerts.action.command;

import com.datatorrent.alerts.ActionHandler;

public class ExecuteCommandHandler implements ActionHandler<ExecuteCommandTuple> {
  private CommandManager scriptManager = new CommandManager();
  
  @Override
  public void handle(ExecuteCommandTuple tuple) {
    scriptManager.executeCommand(tuple.getAppName(), tuple.getCommand(), tuple.getParameters());
  }
}