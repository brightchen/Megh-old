package com.datatorrent.demos.telcom.generate;

public class MsisdnGenerator extends StringComposeGenerator{
  @SuppressWarnings("unchecked")
  public MsisdnGenerator()
  {
    super( new EnumStringRandomGenerator(new String[]{"01"}), 
        new EnumStringRandomGenerator(new String[]{"408", "650", "510", "415", "925", "707"}),
        new FixLengthStringRandomGenerator(CharRandomGenerator.digitCharGenerator, 7) );
  }

}
