package com.datatorrent.demos.telcom.generate;

import java.util.Random;

import com.datatorrent.demos.telcom.model.CallDetailRecord;

public class CallDetailRecordRandomGenerator {
  private Random random = new Random();
  private CharRandomGenerator digitCharGenerator = new CharRandomGenerator(CharRange.digits);
  private FixLengthStringRandomGenerator msidnGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 12);
  private FixLengthStringRandomGenerator imsiGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 15);
  private FixLengthStringRandomGenerator imeiGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 12);
  private EnumStringRandomGenerator planGenerator = new EnumStringRandomGenerator(new String[]{"PLAN1", "PLAN2", "PLAN3", "PLAN4"});
  private EnumStringRandomGenerator callTypeGenerator = new EnumStringRandomGenerator(CallType.labels());
  private EnumStringRandomGenerator correspTypeGenerator = new EnumStringRandomGenerator(new String[]{"CUST1", "CUST2", "CUST3"});
  private FixLengthStringRandomGenerator correspIsdnGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 10);
  //private String durationCallCompleteGenerator;
  //private String bytesGenerator;
  //private String drGenerator;  //disconnect reason
  //private String timeGenerator;  //HH:MM:SS
  //private String dateGenerator;  //MM/DD/YYYY
  
  public CallDetailRecord next()
  {
    CallDetailRecord record = new CallDetailRecord();
    record.setMsidn(msidnGenerator.next());
    record.setImsi(imsiGenerator.next());
    record.setImei(imeiGenerator.next());
    record.setPlan(planGenerator.next());
    record.setCallType(callTypeGenerator.next());
    record.setCorrespType(correspTypeGenerator.next());
    record.setCorrespIsdn(correspIsdnGenerator.next());
    return record;
  }
}
