package com.datatorrent.demos.telcom.generate;

import java.util.List;

import com.datatorrent.demos.telcom.model.CustomerInfo;
import com.google.common.collect.Lists;

public class CustomerInfoRandomGenerator  implements Generator<CustomerInfo>{
  private final MsisdnGenerator msidnGenerator = new MsisdnGenerator();
  private final ImsiGenerator imsiGenerator = new ImsiGenerator();
  private final ImeiGenerator imeiGenerator = new ImeiGenerator();
  
  private final int[] deviceNumArray = {1,1,1,1,1,1,2,2,3};
  
  @Override
  public CustomerInfo next() {
    //most customer only have one device.
    int deviceNumIndex = Generator.random.nextInt(deviceNumArray.length);
    int deviceNum = deviceNumArray[deviceNumIndex];
    List<String> imeis = Lists.newArrayList();
    for(int i=0; i<deviceNum; ++i)
    {
      imeis.add(imeiGenerator.next());
    }
    return new CustomerInfo(msidnGenerator.next(), imsiGenerator.next(), imeis);
  }
}
