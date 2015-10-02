package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCDRHBaseConfig extends DataWarehouseConfig{
  public static EnrichedCDRHBaseConfig instance = new EnrichedCDRHBaseConfig();
  
  protected EnrichedCDRHBaseConfig()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCdrEnrichedRecordTableName();
  }
}
