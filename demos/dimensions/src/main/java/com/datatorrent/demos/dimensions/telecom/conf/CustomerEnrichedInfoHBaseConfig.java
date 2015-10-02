package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerEnrichedInfoHBaseConfig  extends DataWarehouseConfig{
  public static CustomerEnrichedInfoHBaseConfig instance = new CustomerEnrichedInfoHBaseConfig();
  
  protected CustomerEnrichedInfoHBaseConfig()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerEnrichedInfoTableName();
  }
}