package com.datatorrent.demos.dimensions.telecom.generate;

import com.datatorrent.demos.dimensions.telecom.model.CustomerService;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService.IssueType;

public class CustomerServiceRandomGenerator implements Generator<CustomerService>{
  public static final int MAX_DURATION = 100;
  private ImsiGenerator imsiGenerator = new ImsiGenerator();
  
  @Override
  public CustomerService next() {
    String imsi = imsiGenerator.next();
    int totalDuration = Generator.random.nextInt(MAX_DURATION);
    int wait = (int)(totalDuration * Math.random());
    String zipCode = PointZipCodeRepo.instance().getRandomZipCode();
    IssueType issueType = IssueType.values()[Generator.random.nextInt(IssueType.values().length)];
    boolean satisfied = ( Generator.random.nextInt(1) == 0 );
    return new CustomerService(imsi, totalDuration, wait, zipCode, issueType, satisfied);
  }

}
