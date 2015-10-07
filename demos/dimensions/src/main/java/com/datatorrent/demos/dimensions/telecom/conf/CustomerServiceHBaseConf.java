package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerServiceHBaseConf extends DataWarehouseConfig{
  public static CustomerServiceHBaseConf instance = new CustomerServiceHBaseConf();
  
  protected CustomerServiceHBaseConf()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerServiceTableName();
  }
}
