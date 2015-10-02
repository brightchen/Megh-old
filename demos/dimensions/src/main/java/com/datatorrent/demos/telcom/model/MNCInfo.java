package com.datatorrent.demos.telcom.model;

public class MNCInfo {
  public enum Carrier
  {
    ATT("AT&T"), 
    VZN("Verizon"), 
    TMO("T-Mobile"), 
    SPR("Sprint")
    ;
    public final String operatorCode;
    public final String operatorName;
    
    private Carrier(String operatorName)
    {
      this.operatorCode = name();
      this.operatorName = operatorName;
    }
    
    private Carrier(String operatorCode, String operatorName)
    {
      this.operatorCode = operatorCode;
      this.operatorName = operatorName;
    }
  }
  
  public final int mcc;
  public final int mnc;
  public final Carrier carrier;
  
  public MNCInfo(int mcc, int mnc, Carrier carrier)
  {
    this.mcc = mcc;
    this.mnc = mnc;
    this.carrier = carrier;
  }
  
  public String getMccMnc()
  {
    return String.format("%06d", mcc*1000+mnc);
  }
}