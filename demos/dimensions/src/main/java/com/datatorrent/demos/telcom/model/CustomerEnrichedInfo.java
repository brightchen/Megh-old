package com.datatorrent.demos.telcom.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import com.datatorrent.demos.telcom.generate.MNCRepo;
import com.datatorrent.demos.telcom.generate.TACRepo;
import com.datatorrent.demos.telcom.model.MNCInfo.Carrier;

/**
 * CustomerEnrichedInfo also include the information such as carrier, manufacturer and model
 * @author bright
 *
 */
public class CustomerEnrichedInfo extends CustomerInfo{
  public static class SingleRecord implements Serializable
  {
    private static final long serialVersionUID = -1426132792117389626L;

    public static final Collection<String> fields = Collections.unmodifiableCollection( Arrays.asList("OperatorCode", "OperatorName","Imsi", "isdn", "imei",  "deviceBrand", "deviceModel"));
    
    public final long id;
    public final String imsi;
    public final String isdn;
    public final String imei;
    public final String operatorName;
    public final String operatorCode; //carrier 
    public final String deviceBrand;   
    public final String deviceModel;
    
    protected SingleRecord()
    {
      this.id = 0;
      this.imsi = "";
      this.isdn = "";
      this.imei = "";
      this.operatorName = "";
      this.operatorCode = "";
      this.deviceBrand = "";
      this.deviceModel = "";
    }
    
    public SingleRecord(long id, String imsi, String isdn, String imei, String operatorName, String operatorCode, String deviceBrand, String deviceModel)
    {
      this.id = id;
      this.imsi = imsi;
      this.isdn = isdn;
      this.imei = imei;
      this.operatorName = operatorName;
      this.operatorCode = operatorCode;
      this.deviceBrand = deviceBrand;
      this.deviceModel = deviceModel;
    }
    
    public String getImsi() {
      return imsi;
    }
    public String getIsdn() {
      return isdn;
    }
    public String getImei() {
      return imei;
    }
    public String getOperatorName() {
      return operatorName;
    }
    public String getOperatorCode() {
      return operatorCode;
    }
    public String getDeviceBrand() {
      return deviceBrand;
    }
    public String getDeviceModel() {
      return deviceModel;
    }
    
    
  }
  
  private SingleRecord[] records;
  private AtomicLong id = new AtomicLong(0);
  
//  
//  private String operatorCode;
//  private String operatorName;
//  private String[] brands;   
//  private String[] models;
  
  public CustomerEnrichedInfo()
  {
    super();
  }
  
  public CustomerEnrichedInfo(CustomerInfo ci)
  {
    this(ci.imsi, ci.msisdn, ci.imeis);
  }
  
  public CustomerEnrichedInfo(String imsi, String msisdn, Collection<String> imeis)
  {
    super(imsi, msisdn, imeis);
    enrichInfo();
  }

  public void enrichInfo()
  {
    final int imeiSize = imeis.size();
    records = new SingleRecord[imeiSize];
    Carrier carrier = MNCRepo.instance().getMncInfoByImsi(imsi).carrier;
    
    String[] imeiArray = imeis.toArray(new String[imeiSize]);
    for(int i=0; i<imeiSize; ++i)
    {
      TACInfo tacInfo = TACRepo.instance().getTacInfoByImei(imeiArray[i]);
      long curId = id.incrementAndGet();
      records[i] = new SingleRecord(curId, imsi, msisdn, imeiArray[i], carrier.operatorName, carrier.operatorCode, tacInfo.manufacturer, tacInfo.model);
    }
    
  }
  
  public SingleRecord[] getRecords()
  {
    return records;
  }
  
}
