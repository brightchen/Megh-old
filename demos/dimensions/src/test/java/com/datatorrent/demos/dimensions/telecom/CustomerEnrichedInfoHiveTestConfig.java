package com.datatorrent.demos.dimensions.telecom;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHiveConfig;

public class CustomerEnrichedInfoHiveTestConfig extends CustomerEnrichedInfoHiveConfig{
  public static CustomerEnrichedInfoHiveTestConfig instance = new CustomerEnrichedInfoHiveTestConfig();
  
  protected CustomerEnrichedInfoHiveTestConfig()
  {
    host = "localhost";
  }
}
