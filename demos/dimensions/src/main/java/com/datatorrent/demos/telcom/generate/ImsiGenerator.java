package com.datatorrent.demos.telcom.generate;

/**
 * IMSI = MCC + MNC + MSIN
 * 
 * @author bright
 *
 */
public class ImsiGenerator implements Generator<String>{
  private CharRandomGenerator digitCharGenerator = new CharRandomGenerator(CharRange.digits);
  private FixLengthStringRandomGenerator msinGenerator = new FixLengthStringRandomGenerator(digitCharGenerator, 9);
  
  public ImsiGenerator(){}

  @Override
  public String next() {
    return MNCRepo.instance().getRandomMncInfo().getMccMnc() + msinGenerator.next();
  }
  
}
