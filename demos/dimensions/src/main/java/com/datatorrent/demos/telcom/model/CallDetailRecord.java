package com.datatorrent.demos.telcom.model;

import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.demos.telcom.generate.DisconnectReason;

public class CallDetailRecord {
  public static final String delimiter = ";";
  public static final int COLUMN_NUM = 14;
  
  private String isdn;
  private String imsi;
  private String imei;
  private String plan;
  private String callType;
  private String correspType;
  private String correspIsdn;
  private int duration;
  private int bytes;
  private int dr; // disconnect reason code
  private float lat;
  private float lon;
  private long time;

  public CallDetailRecord(){}
  
  public CallDetailRecord(Map<String, byte[]> nameValueMap)
  {
    this.setIsdn(Bytes.toString(nameValueMap.get("isdn")));
    this.setImsi(Bytes.toString(nameValueMap.get("imsi")));
    this.setImei(Bytes.toString(nameValueMap.get("imei")));
    this.setCallType(Bytes.toString(nameValueMap.get("callType")));
    this.setCorrespType(Bytes.toString(nameValueMap.get("correspType")));
    this.setCorrespIsdn(Bytes.toString(nameValueMap.get("correspIsdn")));
    this.setDuration(Bytes.toInt(nameValueMap.get("duration")));
    this.setBytes(Bytes.toInt(nameValueMap.get("bytes")));
    this.setDr(Bytes.toInt(nameValueMap.get("dr")));
    this.setLat(Bytes.toFloat(nameValueMap.get("lat")));
    this.setLon(Bytes.toFloat(nameValueMap.get("lon")));
    
    this.setTime(Bytes.toString(nameValueMap.get("timeInDay")), Bytes.toString(nameValueMap.get("date")));
  }
  
  public String getIsdn() {
    return isdn;
  }

  public void setIsdn(String isdn) {
    this.isdn = isdn;
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


  public int getDr() {
    return dr;
  }

  public void setDr(DisconnectReason disconnectReason) {
    this.dr = disconnectReason.getCode();
  }
  public void setDr(int dr)
  {
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
  public String getDate()
  {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(time);
    // MM/DD/YYYY
    return String.format("%02d/%02d/%4d", c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.YEAR));
  }
  public void setDate(String date)
  {
    throw new RuntimeException("unsupported");
  }
  public String getTimeInDay()
  {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(time);

    // hh:mm:ss
    return String.format("%02d:%02d:%02d", c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND));
  }
  public void setTimeInDay(String timeInDay)
  {
    throw new RuntimeException("unsupported");
  }
  
  public void setTime(long time) {
    this.time = time;
  }

  // hh:mm:ss, MM/DD/YYYY
  public void setTime(String timeInDay, String day) {
    Calendar c = Calendar.getInstance();
    String[] hms = timeInDay.split(":");
    String[] ymd = day.split("/");
    c.set(Integer.valueOf(ymd[0]), Integer.valueOf(ymd[1]), Integer.valueOf(ymd[2]), Integer.valueOf(hms[0]),
        Integer.valueOf(hms[1]), Integer.valueOf(hms[2]));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(isdn).append(delimiter);
    sb.append(imsi).append(delimiter);
    sb.append(imei).append(delimiter);
    sb.append(plan).append(delimiter);
    sb.append(callType).append(delimiter);
    sb.append(correspType).append(delimiter);
    sb.append(correspIsdn).append(delimiter);
    sb.append(duration).append(delimiter);
    if (bytes != 0)
      sb.append(bytes);
    sb.append(delimiter);
    if (dr != 0)
      sb.append(dr);
    sb.append(delimiter);

    sb.append(String.format("%.4f", lat)).append(delimiter);
    sb.append(String.format("%.4f", lon)).append(delimiter);
    
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(time);
    // hh:mm:ss
    sb.append(
        String.format("%02d:%02d:%02d", c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND)))
        .append(delimiter);
    // MM/DD/YYYY
    sb.append(
        String.format("%02d/%02d/%4d", c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.YEAR)));

    return sb.toString();
  }

  public static CallDetailRecord fromLine(String line) {
    if (line.endsWith("\n"))
      line = line.substring(0, line.length() - 1);
    if (line.isEmpty())
      return null;

    CallDetailRecord record = new CallDetailRecord();
    record.setFromLine(line);

    return record;
  }
  
  public String toLine() {
    return toString() + "\n";
  }

  public void setFromLine(String line) {
    try {
      if (line.endsWith("\n"))
        line = line.substring(0, line.length() - 1);
      if (line.isEmpty())
        throw new IllegalArgumentException("The line is empty.");

      String[] items = line.split(delimiter);
      if(items.length != COLUMN_NUM)
      {
        throw new IllegalArgumentException("Column not correct, expect: " + COLUMN_NUM + "; actual: " + items.length + ".\n line: " + line);
      }
      int index = 0;
      setIsdn(items[index++]);
      setImsi(items[index++]);
      setImei(items[index++]);
      setPlan(items[index++]);
      setCallType(items[index++]);
      setCorrespType(items[index++]);
      setCorrespIsdn(items[index++]);
      setDuration(Integer.valueOf(items[index++]));
      if (items[index].length() > 0)
        setBytes(Integer.valueOf(items[index]));
      ++index;
      if (items[index].length() > 0)
        setDr(DisconnectReason.fromCode(Integer.valueOf(items[index])));
      ++index;
      setLat(Float.valueOf(items[index++]));
      setLon(Float.valueOf(items[index++]));

      // hh:mm:ss, MM/DD/YYYY
      setTime(items[index++], items[index++]);
    } catch (Exception e) {
      throw new IllegalArgumentException("The line can't convert to Call Detail Record: " + line, e);
    }
  }
  
  /**
   * for demension computation
   */
  public int getTerminatedAbnomally()
  {
    return DisconnectReason.CallDropped.getCode() == dr ? 1 : 0;
  }
  public int getTerminatedNomally()
  {
    return DisconnectReason.CallComplete.getCode() == dr ? 1 : 0;
  }
  public int getCalled()
  {
    return (DisconnectReason.CallComplete.getCode() == dr || DisconnectReason.CallDropped.getCode() == dr) ? 1 : 0;
  }
}
