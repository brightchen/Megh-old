package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerServiceCassandraConf extends DataWarehouseConfig{
  public static CustomerServiceCassandraConf instance = new CustomerServiceCassandraConf();
  
  protected CustomerServiceCassandraConf()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerServiceTableName();
  }
}