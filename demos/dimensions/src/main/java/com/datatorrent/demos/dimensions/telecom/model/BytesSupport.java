package com.datatorrent.demos.dimensions.telecom.model;

import com.datatorrent.netlet.util.Slice;

public interface BytesSupport
{
  /**
   * convert this object to bytes
   * @return
   */
  public byte[] toBytes();
  public Slice toBytes(byte[] bytes);
}
