package com.datatorrent.alerts.action.command;

public class NoPermissionException extends Exception{
  private static final long serialVersionUID = 4242124281330242225L;

  public NoPermissionException(String message)
  {
    super(message);
  }
}
