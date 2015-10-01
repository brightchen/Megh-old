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
  public EnrichedCDR(){}
  
  public static EnrichedCDR fromCallDetailRecord(String line)
  {
    EnrichedCDR enrichedCDR = new EnrichedCDR();
    enrichedCDR.setFromLine(line);
    return enrichedCDR;
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(delimiter);
    
    //DR
    if(getDr() != 0)
      sb.append(DisconnectReason.fromCode(getDr()).getLabel());
    sb.append(delimiter);
    
    //carrier;
    MNCInfo mncInfo = MNCRepo.instance().getMncInfoByImsi(this.getImsi());
    sb.append(mncInfo.carrier).append(delimiter);
    
    //manufacturer & model
    TACInfo tacInfo = TACRepo.instance().getTacInfoByImei(this.getImei());
    sb.append(tacInfo.manufacturer).append(delimiter);
    sb.append(tacInfo.model).append(delimiter);
    
    return sb.toString();
  }
  
  public String toLine()
  {
    return toString() + "\n";
  }
}
