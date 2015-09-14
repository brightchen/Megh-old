/**
 * This class defines the tuple which will send from the alert storage module to the alert action module
 */
package com.datatorrent.alerts;

public class ActionTuple {
  public static enum ActionType
  {
    NOTIFY_EMAIL,
    EXECUTE_SCRIPT,
  }
  
  protected ActionType action;
  protected int level;
  protected String appName;
  
  
  public ActionType getAction() {
    return action;
  }
  public void setAction(ActionType action) {
    this.action = action;
  }
  public int getLevel() {
    return level;
  }
  public void setLevel(int level) {
    this.level = level;
  }
  public String getAppName() {
    return appName;
  }
  public void setAppName(String appName) {
    this.appName = appName;
  }
  
  
}
