package com.datatorrent.alerts.action.command;

public interface CommandExecutor {
  public void executeCommand(String appName, String command, String[] parameters);
}
