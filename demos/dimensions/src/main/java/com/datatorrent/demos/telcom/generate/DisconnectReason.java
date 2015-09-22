package com.datatorrent.demos.telcom.generate;

public enum DisconnectReason {
  NoResponse(9, "No Response"),
  CallComplete(10, "Call Complete"),
  CallDropped(11, "Call Dropped")
  ;
  
  private int code;
  private String label;
  
  private DisconnectReason(int code, String label)
  {
    this.code = code;
    this.label = label;
  }

  public int getCode() {
    return code;
  }

  public String getLabel() {
    return label;
  }
  
  
}
