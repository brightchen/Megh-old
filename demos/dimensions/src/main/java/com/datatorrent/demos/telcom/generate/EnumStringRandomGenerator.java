package com.datatorrent.demos.telcom.generate;

import java.util.Random;

public class EnumStringRandomGenerator {
  protected static final Random random = new Random();
  protected String[] candidates;
  
  public EnumStringRandomGenerator(String[] candidates)
  {
    if(candidates == null || candidates.length == 0)
      throw new IllegalArgumentException("candidates can't null or empty.");
    this.candidates = candidates;
  }
  
  public String next()
  {
    return candidates[random.nextInt(candidates.length)];
  }
}
