package com.datatorrent.demos.dimensions.telecom.model;

import com.datatorrent.demos.dimensions.telecom.generate.MNCRepo;
import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo;
import com.datatorrent.demos.dimensions.telecom.generate.TACRepo;
import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo.LocationInfo;

public class EnrichedCustomerService extends CustomerService implements BytesSupport
{
  public final String operatorCode;
  public final String deviceBrand;
  public final String deviceModel;

  public final String stateCode;
  public final String state;
  public final String city;
  
  protected EnrichedCustomerService()
  {
    super();
    operatorCode = "";
    deviceBrand = "";
    deviceModel = "";
    stateCode = "";
    state = "";
    city = "";
  }
  
  public EnrichedCustomerService(CustomerService cs, String operatorCode, String deviceBrand, String deviceModel, 
      String stateCode, String state, String city)
  {
    super(cs);
    this.operatorCode = operatorCode;
    this.deviceBrand = deviceBrand;
    this.deviceModel = deviceModel;
    this.stateCode = stateCode;
    this.state = state;
    this.city = city;
  }
  
  public static EnrichedCustomerService fromCustomerService(CustomerService cs)
  {
    //operator code;
    MNCInfo mncInfo = MNCRepo.instance().getMncInfoByImsi(cs.imsi);
    
    //brand & model
    TACInfo tacInfo = TACRepo.instance().getTacInfoByImei(cs.imei);
    
    LocationInfo li = LocationRepo.instance().getLocationInfoByZip(cs.zipCode);

    return new EnrichedCustomerService(cs, mncInfo.carrier.operatorCode, tacInfo.manufacturer, tacInfo.model, li.stateCode, li.state, li.city);
  }
  
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(delimiter);
    
    sb.append(operatorCode).append(delimiter);
    
    sb.append(deviceBrand).append(delimiter);
    sb.append(deviceModel).append(delimiter);
    
    sb.append(stateCode).append(delimiter);
    sb.append(state).append(delimiter);
    sb.append(city);
    
    return sb.toString();
  }
  
  @Override
  public byte[] toBytes()
  {
    return toLine().getBytes();
  }
  
  public String toLine()
  {
    return toString() + "\n";
  }

}
