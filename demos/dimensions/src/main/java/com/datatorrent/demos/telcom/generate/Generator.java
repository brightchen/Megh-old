package com.datatorrent.demos.telcom.generate;

public interface Generator<T> {
  public T next();
}
