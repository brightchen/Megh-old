package com.datatorrent.demos.dimensions.telecom.model;

import com.datatorrent.demos.dimensions.telecom.generate.MNCRepo;
import com.datatorrent.demos.dimensions.telecom.generate.TACRepo;

public class EnrichedCustomerService extends CustomerService
{
  public final String operatorCode;
  public final String deviceBrand;
  public final String deviceModel;
  
  protected EnrichedCustomerService()
  {
    super();
    operatorCode = "";
    deviceBrand = "";
    deviceModel = "";
  }
  
  public EnrichedCustomerService(CustomerService cs, String operatorCode, String deviceBrand, String deviceModel)
  {
    super(cs);
    this.operatorCode = operatorCode;
    this.deviceBrand = deviceBrand;
    this.deviceModel = deviceModel;
  }
  
  public static EnrichedCustomerService fromCustomerService(CustomerService cs)
  {
    //operator code;
    MNCInfo mncInfo = MNCRepo.instance().getMncInfoByImsi(cs.imsi);
    
    //brand & model
    TACInfo tacInfo = TACRepo.instance().getTacInfoByImei(cs.imei);
    
    return new EnrichedCustomerService(cs, mncInfo.carrier.operatorCode, tacInfo.manufacturer, tacInfo.model);
  }
  
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(delimiter);
    
    sb.append(operatorCode).append(delimiter);
    
    sb.append(deviceBrand).append(delimiter);
    sb.append(deviceModel);
    
    return sb.toString();
  }
  
  public String toLine()
  {
    return toString() + "\n";
  }

}
