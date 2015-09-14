package com.datatorrent.alerts.action.command;

import com.datatorrent.alerts.ActionHandler;

public class ExecuteCommandHandler implements ActionHandler<ExecuteCommandTuple> {
  private CommandManager commandManager = new CommandManager();
  
  @Override
  public void handle(ExecuteCommandTuple tuple) {
    commandManager.executeCommand(tuple.getAppName(), tuple.getCommand(), tuple.getParameters());
  }
}