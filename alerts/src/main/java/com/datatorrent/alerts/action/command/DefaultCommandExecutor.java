package com.datatorrent.alerts.action.command;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultCommandExecutor execute command in separate processor and in current work context/environment.
 * 
 * @author bright
 *
 */
public class DefaultCommandExecutor implements CommandExecutor{

  private static final Logger logger = LoggerFactory.getLogger(DefaultCommandExecutor.class);
      
  @Override
  public void executeCommand(String appName, String command, String[] parameters) {
    Runtime rt = Runtime.getRuntime();
    final String cl = getCommandLine(appName, command, parameters);
    try {
      Process p = rt.exec(cl);
      logger.info("Executing command '{}' by process {}.", cl, p);
    } catch (IOException e) {
      logger.warn("Execute command '{}' exception: {}", cl, e.getMessage());
    }
  }

  protected String getCommandLine(String appName, String command, String[] parameters)
  {
    if(parameters == null || parameters.length == 0)
      return command;
    StringBuilder cl = new StringBuilder(command);
    cl.append(" ");
    for(String param : parameters)
      cl.append(param).append(" ");
    
    return cl.toString();
  }
}
