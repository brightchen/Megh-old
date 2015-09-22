package com.datatorrent.demos.telcom.model;

public class CallDetailRecord {
  private String msidn;
  private String imsi;
  private String imei;
  private String plan;
  private String callType;
  private String correspType;
  private String correspIsdn;
  private String duration;
  private String bytes;
  private String dr;  //disconnect reason
  private String time;  //HH:MM:SS
  private String date;  //MM/DD/YYYY
  
  public String getMsidn() {
    return msidn;
  }
  public void setMsidn(String msidn) {
    this.msidn = msidn;
  }
  public String getImsi() {
    return imsi;
  }
  public void setImsi(String imsi) {
    this.imsi = imsi;
  }
  public String getImei() {
    return imei;
  }
  public void setImei(String imei) {
    this.imei = imei;
  }
  public String getPlan() {
    return plan;
  }
  public void setPlan(String plan) {
    this.plan = plan;
  }
  public String getCallType() {
    return callType;
  }
  public void setCallType(String callType) {
    this.callType = callType;
  }
  public String getCorrespType() {
    return correspType;
  }
  public void setCorrespType(String correspType) {
    this.correspType = correspType;
  }
  public String getCorrespIsdn() {
    return correspIsdn;
  }
  public void setCorrespIsdn(String correspIsdn) {
    this.correspIsdn = correspIsdn;
  }
  public String getDuration() {
    return duration;
  }
  public void setDuration(String duration) {
    this.duration = duration;
  }
  public String getBytes() {
    return bytes;
  }
  public void setBytes(String bytes) {
    this.bytes = bytes;
  }
  public String getDr() {
    return dr;
  }
  public void setDr(String dr) {
    this.dr = dr;
  }
  public String getTime() {
    return time;
  }
  public void setTime(String time) {
    this.time = time;
  }
  public String getDate() {
    return date;
  }
  public void setDate(String date) {
    this.date = date;
  }

  
}
