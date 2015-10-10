package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerEnrichedInfoCassandraConfig  extends DataWarehouseConfig{
  public static CustomerEnrichedInfoCassandraConfig instance = new CustomerEnrichedInfoCassandraConfig();
  
  protected CustomerEnrichedInfoCassandraConfig()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getCassandraPort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerEnrichedInfoTableName();
  }
}