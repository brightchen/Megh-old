package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Random;

public interface Generator<T>
{
  public static final Random random = new Random();

  public T next();
}
