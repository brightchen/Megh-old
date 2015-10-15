package com.datatorrent.demos.dimensions.telecom.model;

import java.util.Calendar;

public class CustomerService {
  public static final String delimiter = ";";
  
  public static enum IssueType
  {
    DeviceUpgrade,
    CallQuality,
    DeviceQuality,
    Billing,
    NetworkCoverage,
    Roaming
  }
  
  public final String imsi;
  public final String isdn;
  public final String imei;
  public final int totalDuration;
  public final int wait;
  public final String zipCode;
  public final IssueType issueType;
  public final boolean satisfied;
  public final long time = Calendar.getInstance().getTimeInMillis();
  
  protected CustomerService()
  {
    imsi = "";
    isdn = "";
    imei = "";
    totalDuration = 0;
    wait = 0;
    zipCode = "";
    issueType = null;
    satisfied = false;
  }
  
  public CustomerService(String imsi, String isdn, String imei, int totalDuration, int wait, String zipCode, IssueType issueType, boolean satisfied)
  {
    this.imsi = imsi;
    this.isdn = isdn;
    this.imei = imei;
    this.totalDuration = totalDuration;
    this.wait = wait;
    this.zipCode = zipCode;
    this.issueType = issueType;
    this.satisfied = satisfied;
  }
  
  public CustomerService( CustomerService other)
  {
    this(other.imsi, other.isdn, other.imei, other.totalDuration, other.wait, other.zipCode, other.issueType, other.satisfied);
  }

  public int getServiceCallCount()
  {
    return 1;
  }
  
  public int getWait()
  {
    return wait;
  }

  public String getZipCode()
  {
    return zipCode;
  }

  public long getTime()
  {
    return time;
  }

}
