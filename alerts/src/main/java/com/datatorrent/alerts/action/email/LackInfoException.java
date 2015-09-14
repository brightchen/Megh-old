package com.datatorrent.alerts.action.email;

public class LackInfoException extends Exception{
  private static final long serialVersionUID = -7662686151451704723L;

  public LackInfoException(String message)
  {
    super(message);
  }
}
