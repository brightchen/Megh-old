package com.datatorrent.demos.telcom.model;

import java.util.Calendar;

import com.datatorrent.demos.telcom.generate.DisconnectReason;

public class CallDetailRecord {
  private String msidn;
  private String imsi;
  private String imei;
  private String plan;
  private String callType;
  private String correspType;
  private String correspIsdn;
  private int duration;
  private int bytes;
  private DisconnectReason dr;  //disconnect reason
  private float lat;
  private float lon;
  private long time; 
  
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
  public int getDuration() {
    return duration;
  }
  public void setDuration(int duration) {
    this.duration = duration;
  }
  public int getBytes() {
    return bytes;
  }
  public void setBytes(int bytes) {
    this.bytes = bytes;
  }
  public DisconnectReason getDr() {
    return dr;
  }
  public void setDr(DisconnectReason dr) {
    this.dr = dr;
  }
  public float getLat() {
    return lat;
  }
  public void setLat(float lat) {
    this.lat = lat;
  }
  public float getLon() {
    return lon;
  }
  public void setLon(float lon) {
    this.lon = lon;
  }
  public long getTime() {
    return time;
  }
  public void setTime(long time) {
    this.time = time;
  }
  
  public String toLine()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(msidn).append(";");
    sb.append(imsi).append(";");
    sb.append(imei).append(";");
    sb.append(plan).append(";");
    sb.append(callType).append(";");
    sb.append(correspType).append(";");
    sb.append(correspIsdn).append(";");
    sb.append(duration).append(";");
    if(bytes != 0 )
      sb.append(bytes);
    sb.append(";");
    if(dr != null)
      sb.append(dr.getCode());
    sb.append(";");
    
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(time);
    
    sb.append(String.format("%.4f", lat)).append(";");
    sb.append(String.format("%.4f", lon)).append(";");
    //hh:mm:ss
    sb.append(String.format("%02d:%02d:%02d", c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND))).append(";");
    //MM/DD/YYYY
    sb.append(String.format("%02d:%02d:%4d", c.get(Calendar.MONTH)+1, c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.YEAR)));
    sb.append("\n");
    
    return sb.toString();
    
  }
}
