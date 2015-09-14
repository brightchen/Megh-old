package com.datatorrent.alerts.action.command;

public interface PermissionManager {
  public void verifyPermission(String appName, String command) throws NoPermissionException;
}
