package com.datatorrent.demos.telcom.hive;

import com.datatorrent.demos.telcom.conf.TelecomDemoConf;

public class CustomerEnrichedInfoHiveConfig extends DataWarehouseConfig{
  public static CustomerEnrichedInfoHiveConfig instance = new CustomerEnrichedInfoHiveConfig();
  
  protected CustomerEnrichedInfoHiveConfig()
  {
    host = TelecomDemoConf.instance.getHiveHost();
    port = TelecomDemoConf.instance.getHivePort();
    userName = TelecomDemoConf.instance.getHiveUserName();
    password = TelecomDemoConf.instance.getHivePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerEnrichedInfoTableName();
  }
}
