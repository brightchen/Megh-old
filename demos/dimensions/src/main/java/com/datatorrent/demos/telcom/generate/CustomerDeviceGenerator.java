package com.datatorrent.demos.telcom.generate;

import com.datatorrent.demos.telcom.model.CustomerDevice;

public class CustomerDeviceGenerator implements Generator<CustomerDevice> {
  private MsisdnGenerator msisdnGenerator = new MsisdnGenerator();
  private ImsiGenerator imsiGenerator = new ImsiGenerator();
  private ImeiGenerator imeiGenerator = new ImeiGenerator();
  
  @Override
  public CustomerDevice next() {
    CustomerDevice cd = new CustomerDevice();
    cd.setIsdn(msisdnGenerator.next());
    cd.setImsi(imsiGenerator.next());
    cd.setImei(imeiGenerator.next());
    return cd;
  }

}
