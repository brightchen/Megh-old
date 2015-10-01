package com.datatorrent.demos.telcom.conf;

import com.datatorrent.demos.telcom.hive.DataWarehouseConfig;

public class CDREnrichedRecordHBaseConfig extends DataWarehouseConfig{
  public static CDREnrichedRecordHBaseConfig instance = new CDREnrichedRecordHBaseConfig();
  
  protected CDREnrichedRecordHBaseConfig()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCdrEnrichedRecordTableName();
  }
}
