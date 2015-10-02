package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.hbase.HBaseFieldInfo;
import com.datatorrent.contrib.hbase.HBasePOJOPutOperator;
import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.TableInfo;

public class CustomerEnrichedInfoHbaseOutputOperator extends HBasePOJOPutOperator{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoHbaseOutputOperator.class);
  
  private DataWarehouseConfig hbaseConfig = CustomerEnrichedInfoHBaseConfig.instance;
  
  private boolean startOver = false;
  
  
  protected void configure()
  {
    //table info
    TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<HBaseFieldInfo>();
    
    tableInfo.setRowOrIdExpression("imsi");

    List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
    fieldsInfo.add( new HBaseFieldInfo( "id", "id", SupportType.STRING, "f0") );
    fieldsInfo.add( new HBaseFieldInfo( "isdn", "isdn", SupportType.STRING, "f0") );
    fieldsInfo.add( new HBaseFieldInfo( "imei", "imei", SupportType.STRING, "f0") );
    fieldsInfo.add( new HBaseFieldInfo( "operatorName", "operatorName", SupportType.STRING, "f1") );
    fieldsInfo.add( new HBaseFieldInfo( "operatorCode", "operatorCode", SupportType.STRING, "f1") );
    fieldsInfo.add( new HBaseFieldInfo( "deviceBrand", "deviceBrand", SupportType.STRING, "f1") );
    fieldsInfo.add( new HBaseFieldInfo( "deviceModel", "deviceModel", SupportType.STRING, "f1") );

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
  public final transient DefaultInputPort<SingleRecord> input = new DefaultInputPort<SingleRecord>()
  {
    @Override
    public void process(SingleRecord t)
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
        tableDescriptor.addFamily(new HColumnDescriptor("f0"));
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
  
