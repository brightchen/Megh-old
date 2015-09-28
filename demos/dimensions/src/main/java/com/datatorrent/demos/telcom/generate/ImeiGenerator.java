package com.datatorrent.demos.telcom.generate;

import java.util.Random;

/**
 * 
 * @author bright
 * 
 * IMEI format: TAC(8 digits) + serial number(6 digits) + checksum(1 digit)
 */
public class ImeiGenerator implements Generator<String>{
  private CharRandomGenerator digitCharGenerator = new CharRandomGenerator(CharRange.digits);
  private FixLengthStringRandomGenerator serialGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 6);
  //TODO: implement checksum later
  private FixLengthStringRandomGenerator checksumGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 1);
  
  public ImeiGenerator(){}

  @Override
  public String next() {
    return TACRepo.instance().getRandomTacInfo().getTacAsString() + serialGenerator.next() + checksumGenerator.next();
  }
  
}
