package com.datatorrent.demos.telcom.model;

import com.datatorrent.demos.telcom.generate.DisconnectReason;
import com.datatorrent.demos.telcom.generate.MNCRepo;
import com.datatorrent.demos.telcom.generate.TACRepo;

/**
 * Append other information
 *   - DR
 *   - carrier
 *   - manufacture, model
 *   
 * @author bright
 *
 */
public class EnrichedCDR extends CallDetailRecord{
  private String drLabel;
  private String operatorCode;
  private String deviceBrand;
  private String deviceModel;
  
  public EnrichedCDR(){}
  
  public static EnrichedCDR fromCallDetailRecord(String line)
  {
    EnrichedCDR enrichedCDR = new EnrichedCDR();
    enrichedCDR.setFromLine(line);
    enrichedCDR.enrich();
    return enrichedCDR;
  }
  
  protected void enrich()
  {
    //DR
    if(getDr() != 0)
      drLabel = DisconnectReason.fromCode(getDr()).getLabel();
    
    //operator code;
    MNCInfo mncInfo = MNCRepo.instance().getMncInfoByImsi(this.getImsi());
    operatorCode = mncInfo.carrier.operatorCode;
    
    //brand & model
    TACInfo tacInfo = TACRepo.instance().getTacInfoByImei(this.getImei());
    deviceBrand = tacInfo.manufacturer;
    deviceModel = tacInfo.model;
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(delimiter);
    
    //DR
    if(drLabel != null)
      sb.append(drLabel);
    sb.append(delimiter);
    
    sb.append(operatorCode).append(delimiter);
    
    sb.append(deviceBrand).append(delimiter);
    sb.append(deviceModel).append(delimiter);
    
    return sb.toString();
  }
  
  public String toLine()
  {
    return toString() + "\n";
  }

  public String getDrLabel() {
    return drLabel;
  }

  public void setDrLabel(String drLabel) {
    this.drLabel = drLabel;
  }

  public String getOperatorCode() {
    return operatorCode;
  }

  public void setOperatorCode(String operatorCode) {
    this.operatorCode = operatorCode;
  }

  public String getDeviceBrand() {
    return deviceBrand;
  }

  public void setDeviceBrand(String deviceBrand) {
    this.deviceBrand = deviceBrand;
  }

  public String getDeviceModel() {
    return deviceModel;
  }

  public void setDeviceModel(String deviceModel) {
    this.deviceModel = deviceModel;
  }
  
  
}
