package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCustomerServiceCassandraConf extends DataWarehouseConfig{
  public static EnrichedCustomerServiceCassandraConf instance = new EnrichedCustomerServiceCassandraConf();
  
  protected EnrichedCustomerServiceCassandraConf()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getEnrichedCustomerServiceTableName();
  }
}
