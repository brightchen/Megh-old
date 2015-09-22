package com.datatorrent.demos.telcom.generate;

public abstract class AbstractStringRandomGenerator {
  protected CharRandomGenerator charGenerator;
  
  public String next()
  {
    if(charGenerator == null)
      throw new RuntimeException("Please set the char generator first.");
    final int stringLen = getStringLength();
    if(stringLen < 0 )
      throw new RuntimeException("The string lenght expect not less than zero.");
    if(stringLen == 0)
      return "";
    char[] chars = new char[stringLen];
    for(int index=0; index<stringLen; ++index)
      chars[index] = charGenerator.next();
    return new String(chars);
  }
  
  protected abstract int getStringLength();
}
