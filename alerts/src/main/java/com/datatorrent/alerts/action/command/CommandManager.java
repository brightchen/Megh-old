package com.datatorrent.alerts.action.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandManager {
  private static final Logger logger = LoggerFactory.getLogger(CommandManager.class);
  
  private final PermissionManager permissionManager = createPermissionManager();
  
  public void executeCommand(String appName, String command, String[] parameters)
  {
    try
    {
      verifyPermission(appName, command);
    }
    catch(NoPermissionException e)
    {
      logger.warn("No Permission to execute command {}", command);
    }
  }
  
  /**
   * @param appName
   * @param command
   * @throws NoPermissionException
   */
  public void verifyPermission(String appName, String command) throws NoPermissionException
  {
    permissionManager.verifyPermission(appName, command);
  }
  
  /**
   * right now, we only support command from white list
   * @return
   */
  protected PermissionManager createPermissionManager()
  {
    return new WhiteListPermissionManager();
  }
}
