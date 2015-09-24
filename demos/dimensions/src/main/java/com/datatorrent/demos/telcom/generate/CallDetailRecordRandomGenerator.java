package com.datatorrent.demos.telcom.generate;

import java.util.Calendar;
import java.util.Random;

import com.datatorrent.demos.telcom.model.CallDetailRecord;

/**
 * record example
MSIDN;       IMSI;           IMEI;        PLAN;CALL_TYPE;CORRESP_TYPE;CORRESP_ISDN;DURATION;BYTES;DR;  LAT;     LONG;      TIME;DATE 
068373748102;208100167682477;351905149071;PLAN1;MOC;     CUST1;       0612287077;  247;     ;     10;  32.9546; -97.015;   12:07:12;01/01/2015 
068373748102;208100167682477;351905149071;PLAN1;MTC;     CUST2;       0600000001;  300;     ;     10;  32.9546; -97.015;   12:15:09;01/01/2015 
068373748102;208100167682477;351905149071;PLAN1;SMS-MO;  CUST1;       0613637193;  0;       ;     ;    32.92748; -96.9595; 12:18:18;01/01/2015 
068373748102;208100167682477;351905149071;PLAN1;SMS-MT;  CUST1;       0612899062;  0;       ;     ;    32.9656;  -96.8816; 12:21:07;01/01/2015 
065978198280;208100310191699;356008289837;PLAN3;MOC;     CUST1;       0612283725;  90;      ;     11;  33.0154;  -96.5501; 12:00:00;01/01/2015 
065978198280;208100310191699;356008289837;PLAN3;MOC;     CUST1;       0613069656;  82;      ;     10;  33.07818; -96.7944; 12:02:27;01/01/2015 
065978198280;208100310191699;356008289837;PLAN3;DATA;    CUST1;       0613481951;  0;       150   ;    33.09851; -96.6374; 12:04:41;01/01/2015 
 */
public class CallDetailRecordRandomGenerator {
  private Random random = new Random();
  private CharRandomGenerator digitCharGenerator = new CharRandomGenerator(CharRange.digits);
  private StringComposeGenerator msidnGenerator = new StringComposeGenerator( new EnumStringRandomGenerator(new String[]{"01"}),  
      new EnumStringRandomGenerator(new String[]{"408", "650", "510", "415", "925", "707"}), new FixLengthStringRandomGenerator(digitCharGenerator, 7) );
  private FixLengthStringRandomGenerator imsiGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 15);
  private FixLengthStringRandomGenerator imeiGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 12);
  private EnumStringRandomGenerator planGenerator = new EnumStringRandomGenerator(new String[]{"PLAN1", "PLAN2", "PLAN3", "PLAN4"});
  private EnumStringRandomGenerator callTypeGenerator = new EnumStringRandomGenerator(CallType.labels());
  private EnumStringRandomGenerator correspTypeGenerator = new EnumStringRandomGenerator(new String[]{"CUST1", "CUST2", "CUST3"});
  private FixLengthStringRandomGenerator correspIsdnGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 10);

  //initial as yesterday.
  private transient volatile long recordTime = Calendar.getInstance().getTimeInMillis() - 24*60*60*1000;
  

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
    
    //duration;bytes;dr
    if(CallType.MOC.label().equals(record.getCallType()) || CallType.MTC.label().equals(record.getCallType()))
    {
      //dr
      record.setDr(DisconnectReason.randomDisconnectReason());
      //duration
      switch(record.getDr())
      {
      case NoResponse:
        record.setDuration(random.nextInt(2)+1);
        break;
      case CallComplete:
        record.setDuration(random.nextInt(298)+2);
        break;
      case CallDropped:
        record.setDuration(random.nextInt(3));
        break;
      }
      //bytes: empty
    }
    else  //sms and data
    {
      record.setDuration(0);
      //dr: empty
      if(CallType.DATA.name().equals(record.getCallType())) // data
      {
        record.setBytes(random.nextInt(1073741824)+1);
      }
    }
    //[35, 41]
    record.setLat((float)(Math.random()*7+35));
    //[-124, -118]
    record.setLon((float)(Math.random()*7-124));
    
    recordTime += random.nextInt(100);
    record.setTime(recordTime);
    
    return record;
  }
  
 
 
}
