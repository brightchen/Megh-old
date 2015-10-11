package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCustomerServiceHBaseConf extends DataWarehouseConfig{
  public static EnrichedCustomerServiceHBaseConf instance = new EnrichedCustomerServiceHBaseConf();
  
  protected EnrichedCustomerServiceHBaseConf()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getEnrichedCustomerServiceTableName();
  }
}
