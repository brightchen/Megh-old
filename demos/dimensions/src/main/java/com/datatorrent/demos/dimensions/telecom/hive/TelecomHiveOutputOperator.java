package com.datatorrent.demos.dimensions.telecom.hive;

import java.util.ArrayList;

import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator;


public class TelecomHiveOutputOperator<T> extends AbstractFSRollingOutputOperator<T>
{

  @Override
  public ArrayList<String> getHivePartition(T arg0)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected byte[] getBytesForTuple(T arg0)
  {
    // TODO Auto-generated method stub
    return null;
  }

}
