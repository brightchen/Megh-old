package com.datatorrent.demos.dimensions.telecom.generate;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CallDetailRecord;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

/**
 * This class generate random CDR from customer information
 * @author bright
 *
 */
public class CallDetailRecordCustomerInfoGenerator implements Generator<CallDetailRecord> {
  protected CustomerEnrichedInfoHbaseRepo repo = CustomerEnrichedInfoHbaseRepo.createInstance(CustomerEnrichedInfoHBaseConfig.instance);
  protected CallDetailRecordRandomGenerator cdrRandomGenerator = new CallDetailRecordRandomGenerator();
  
  @Override
  public CallDetailRecord next() {
    CallDetailRecord cdr = cdrRandomGenerator.next();
    
    //fill with the customer info.
    SingleRecord customerInfo = repo.getRandomCustomerEnrichedInfo();
    cdr.setIsdn(customerInfo.getIsdn());
    cdr.setImsi(customerInfo.getImsi());
    cdr.setImei(customerInfo.getImei());
    
    return cdr;
  }

}
