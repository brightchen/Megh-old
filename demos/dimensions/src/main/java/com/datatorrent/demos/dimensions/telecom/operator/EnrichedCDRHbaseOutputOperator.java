package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.hbase.HBaseFieldInfo;
import com.datatorrent.contrib.hbase.HBasePOJOPutOperator;
import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.generate.CDRHBaseFieldInfo;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.FieldValueGenerator.FieldValueHandler;
import com.datatorrent.lib.util.TableInfo;

public class EnrichedCDRHbaseOutputOperator extends HBasePOJOPutOperator{
  
  private static final transient Logger logger = LoggerFactory.getLogger(EnrichedCDRHbaseOutputOperator.class);
  
  private DataWarehouseConfig hbaseConfig = EnrichedCDRHBaseConfig.instance;
  
  private boolean startOver = false;
  
  protected void configure()
  {
    //table info
    TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<HBaseFieldInfo>();
    
    tableInfo.setRowOrIdExpression("imsi");

    List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
    fieldsInfo.add( new CDRHBaseFieldInfo( "isdn", "isdn", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "imei", "imei", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "plan", "plan", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "callType", "callType", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "correspType", "correspType", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "correspIsdn", "correspIsdn", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "duration", "duration", SupportType.INTEGER, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "bytes", "bytes", SupportType.INTEGER, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "dr", "dr", SupportType.INTEGER, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "lat", "lat", SupportType.FLOAT, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "lon", "lon", SupportType.FLOAT, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "date", "date", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "time", "timeInDay", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "drLabel", "drLabel", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "operatorCode", "operatorCode", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "deviceBrand", "deviceBrand", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "deviceModel", "deviceModel", SupportType.STRING, "f1") );
    fieldsInfo.add( new CDRHBaseFieldInfo( "zipCode", "zipCode", SupportType.STRING, "f1") );
 
    tableInfo.setFieldsInfo(fieldsInfo);
    setTableInfo(tableInfo);

    //store
    HBaseStore store = new HBaseStore();
    store.setTableName(hbaseConfig.getTableName());
    store.setZookeeperQuorum(hbaseConfig.getHost());
    store.setZookeeperClientPort(hbaseConfig.getPort());

    setStore(store);

  }
  
  
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<EnrichedCDR> input = new DefaultInputPort<EnrichedCDR>()
  {
    @Override
    public void process(EnrichedCDR t)
    {
      processTuple(t);
    }
  };
  
  @Override
  public void processTuple(Object tuple) {
    super.processTuple(tuple);
  }
  
  private boolean initialized = false;
  public void initialize()
  {
  //create table;
    try
    {
      configure();
      getStore().connect();
      Thread.sleep(100);
      createTable();
      
      initialized = true;
    }
    catch(Exception e)
    {
      logger.error("create table '{}' failed.\n exception: {}", hbaseConfig.getTableName(), e.getMessage());
    }
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    if(!initialized)
      initialize();
  }
  

  protected void createTable() throws Exception
  {
    HBaseAdmin admin = null;
    try
    {
      admin = new HBaseAdmin(store.getConfiguration());
      final String tableName = store.getTableName();
      
      boolean hasTable = admin.isTableAvailable(tableName);
          
      if(hasTable && startOver)
      {
        admin.disableTable(tableName);
        admin.deleteTable( tableName );
        hasTable = false;
      }
      if(!hasTable)
      {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }
      
    }
    catch (Exception e)
    {
      logger.error("exception", e);
      throw e;
    }
    finally
    {
      if (admin != null)
      {
        try
        {
          admin.close();
        }
        catch (Exception e)
        {
          logger.warn("close admin exception. ", e);
        }
      }
    }
   
  }


  public DataWarehouseConfig getHbaseConfig() {
    return hbaseConfig;
  }


  public void setHbaseConfig(DataWarehouseConfig hbaseConfig) {
    this.hbaseConfig = hbaseConfig;
  }


  public boolean isStartOver() {
    return startOver;
  }


  public void setStartOver(boolean startOver) {
    this.startOver = startOver;
  }
  
  
}