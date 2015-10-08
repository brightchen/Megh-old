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
  protected CustomerEnrichedInfoHbaseRepo customerEnrichedInfoHbaseRepo = null;
  protected CallDetailRecordRandomGenerator cdrRandomGenerator = new CallDetailRecordRandomGenerator();
  
  @Override
  public CallDetailRecord next() {
    if(customerEnrichedInfoHbaseRepo == null)
      customerEnrichedInfoHbaseRepo = CustomerEnrichedInfoHbaseRepo.createInstance(CustomerEnrichedInfoHBaseConfig.instance);
    
    CallDetailRecord cdr = cdrRandomGenerator.next();
    
    //fill with the customer info.
    SingleRecord customerInfo = customerEnrichedInfoHbaseRepo.getRandomCustomerEnrichedInfo();
    cdr.setIsdn(customerInfo.getIsdn());
    cdr.setImsi(customerInfo.getImsi());
    cdr.setImei(customerInfo.getImei());
    
    return cdr;
  }

}
