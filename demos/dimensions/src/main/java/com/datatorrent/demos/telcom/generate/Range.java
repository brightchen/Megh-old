package com.datatorrent.demos.telcom.generate;

public class Range<T> {
  public final T from;
  public final T to;
  
  public Range(T from, T to)
  {
    this.from = from;
    this.to = to;
  }
}
