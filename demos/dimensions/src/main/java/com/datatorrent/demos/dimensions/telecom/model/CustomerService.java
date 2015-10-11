package com.datatorrent.demos.dimensions.telecom.model;

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
  public final int totalDuration;
  public final int wait;
  public final String zipCode;
  public final IssueType issueType;
  public final boolean satisfied;
  
  protected CustomerService()
  {
    imsi = "";
    totalDuration = 0;
    wait = 0;
    zipCode = "";
    issueType = null;
    satisfied = false;
  }
  
  public CustomerService(String imsi, int totalDuration, int wait, String zipCode, IssueType issueType, boolean satisfied)
  {
    this.imsi = imsi;
    this.totalDuration = totalDuration;
    this.wait = wait;
    this.zipCode = zipCode;
    this.issueType = issueType;
    this.satisfied = satisfied;
  }
  
  public CustomerService( CustomerService other)
  {
    this(other.imsi, other.totalDuration, other.wait, other.zipCode, other.issueType, other.satisfied);
  }

  public String getZipCodeAsString()
  {
    return String.valueOf(zipCode);
  }
  
  public int getServiceCallCount()
  {
    return 1;
  }
  
  public int getWait()
  {
    return wait;
  }
}
