package com.datatorrent.demos.telcom.generate;

import java.util.Random;

public interface Generator<T> {
  public static final Random random = new Random();
  
  public T next();
}
