package com.datatorrent.demos.telcom.model;

public class MNCInfo {
  public final int mcc;
  public final int mnc;
  public final String carrier;
  
  public MNCInfo(int mcc, int mnc, String carrier)
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
