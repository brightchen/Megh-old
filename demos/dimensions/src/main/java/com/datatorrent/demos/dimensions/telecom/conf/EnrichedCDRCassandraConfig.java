package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCDRCassandraConfig extends DataWarehouseConfig{
  public static EnrichedCDRCassandraConfig instance = new EnrichedCDRCassandraConfig();
  
  protected EnrichedCDRCassandraConfig()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getCassandraPort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCdrEnrichedRecordTableName();
  }
}
