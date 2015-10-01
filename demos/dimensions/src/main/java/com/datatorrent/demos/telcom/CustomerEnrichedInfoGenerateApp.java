package com.datatorrent.demos.telcom;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hive.FSPojoToHiveOperator;
import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;
import com.datatorrent.contrib.hive.FSPojoToHiveOperator.FIELD_TYPE;
import com.datatorrent.contrib.hive.HiveMockTest.InnerObj;
import com.datatorrent.demos.dimensions.telecom.CustomerEnrichedInfoHiveTestConfig;
import com.datatorrent.demos.telcom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.telcom.generate.CustomerEnrichedInfoGenerateOperator;
import com.datatorrent.demos.telcom.generate.CustomerEnrichedInfoHbaseOutputOperator;
import com.datatorrent.demos.telcom.generate.CustomerEnrichedInfoHiveOutputOperator;
import com.datatorrent.demos.telcom.hive.CustomerEnrichedInfoHiveConfig;
import com.datatorrent.demos.telcom.hive.DataWarehouseConfig;
import com.datatorrent.demos.telcom.model.CustomerEnrichedInfo;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * This application generate random customer info and write to Hive database.
 * The generation of CDR depends on the customer info.
 * So, if the customer info changed, all previous generated CDR are not valid any more.
 * 
 * @author bright
 *
 */
@ApplicationAnnotation(name="CustomerInfoGenerateApp")
public class CustomerEnrichedInfoGenerateApp implements StreamingApplication {
  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Hive = 0x10;
  
  protected int outputMask = outputMask_HBase;
  
  protected String fileDir = "CEI";
  
  
  protected DataWarehouseConfig hiveConfig = CustomerEnrichedInfoHiveConfig.instance;
  protected DataWarehouseConfig hbaseConfig = CustomerEnrichedInfoHBaseConfig.instance;
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    CustomerEnrichedInfoGenerateOperator generator = new CustomerEnrichedInfoGenerateOperator();
    dag.addOperator("CustomerEnrichedInfo-Generator", generator);
    
    //use HBase
    if((outputMask & outputMask_HBase) != 0)
    {
      CustomerEnrichedInfoHbaseOutputOperator hbaseOutput = new CustomerEnrichedInfoHbaseOutputOperator();
      hbaseOutput.setHbaseConfig(hbaseConfig);
      dag.addOperator("HBase Ouput", hbaseOutput);
      dag.addStream("HBase Stream", generator.outputPort, hbaseOutput.input);
    }
    //use Hive
    if((outputMask & outputMask_Hive) != 0)
    {
      //configure this operator
      CustomerEnrichedInfoHiveOutputOperator hiveOutput = createHiveOutput();
      hiveOutput.setHiveConfig(CustomerEnrichedInfoHiveTestConfig.instance);
      
      dag.addOperator("Hive Ouput", hiveOutput);
      dag.addStream("Hive Stream", generator.outputPort, hiveOutput.input);
    }
  }

  protected CustomerEnrichedInfoHiveOutputOperator createHiveOutput()
  {
    CustomerEnrichedInfoHiveOutputOperator hiveOutput = new CustomerEnrichedInfoHiveOutputOperator();
    hiveOutput.setHiveConfig(hiveConfig);
    
    return hiveOutput;
  }
  
  protected FSPojoToHiveOperator createFsToHiveOutput()
  {
    FSPojoToHiveOperator fsRolling = new FSPojoToHiveOperator();
    fsRolling.setFilePath(fileDir);
    
    short permission = 511;
    fsRolling.setFilePermission(permission);
    
    //columns and types
    fsRolling.setHiveColumns(new ArrayList(CustomerEnrichedInfo.SingleRecord.fields));
    
    ArrayList<FIELD_TYPE> fieldtypes = new ArrayList<FIELD_TYPE>();
    for(int i=0; i<CustomerEnrichedInfo.SingleRecord.fields.size(); ++i)
      fieldtypes.add(FIELD_TYPE.STRING);
    fsRolling.setHiveColumnDataTypes(fieldtypes);
    
    
//    ArrayList<FIELD_TYPE> partitiontypes = new ArrayList<FIELD_TYPE>();
//    partitiontypes.add(FIELD_TYPE.STRING);
//    
//    //expression
//    ArrayList<String> expressions = new ArrayList<String>();
//    expressions.add("getId()");
//
//    fsRolling.setHivePartitionColumnDataTypes(partitiontypes);
//    //ArrayList<FIELD_TYPE> partitionColumnType = new ArrayList<FIELD_TYPE>();
//    //partitionColumnType.add(FIELD_TYPE.STRING);
//    fsRolling.setHivePartitionColumns(hivePartitionColumns);
//    // fsRolling.setHivePartitionColumnsDataTypes(partitionColumnType);
//    
//    ArrayList<String> expressionsPartitions = new ArrayList<String>();
//
//    expressionsPartitions.add("getDate()");
//    
//    fsRolling.setMaxLength(128);
//    fsRolling.setExpressionsForHiveColumns(expressions);
//    fsRolling.setExpressionsForHivePartitionColumns(expressionsPartitions);
//    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
//    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
//    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
//    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
//    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
//
//    fsRolling.setup(context);
//    hiveOperator.setup(context);
//    FilePartitionMapping mapping1 = new FilePartitionMapping();
//    FilePartitionMapping mapping2 = new FilePartitionMapping();
//    mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-11" + "/" + "0-transaction.out.part.0");
//    ArrayList<String> partitions1 = new ArrayList<String>();
//    partitions1.add("2014-12-11");
//    mapping1.setPartition(partitions1);
//    ArrayList<String> partitions2 = new ArrayList<String>();
//    partitions2.add("2014-12-12");
//    mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-12" + "/" + "0-transaction.out.part.0");
//    mapping2.setPartition(partitions2);
//    for (int wid = 0, total = 0;
//            wid < NUM_WINDOWS;
//            wid++) {
//      fsRolling.beginWindow(wid);
//      for (int tupleCounter = 1;
//              tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
//              tupleCounter++, total++) {
//        InnerObj innerObj = new InnerObj();
//        innerObj.setId(tupleCounter);
//        innerObj.setDate("2014-12-1" + tupleCounter);
//        fsRolling.input.process(innerObj);
//      }
//      if (wid == 7) {
//        fsRolling.committed(wid - 1);
//        hiveOperator.processTuple(mapping1);
//        hiveOperator.processTuple(mapping2);
//      }
//
//      fsRolling.endWindow();
//    }

    return fsRolling;
  }

  public String getFileDir() {
    return fileDir;
  }

  public void setFileDir(String fileDir) {
    this.fileDir = fileDir;
  }

  public DataWarehouseConfig getHiveConfig() {
    return hiveConfig;
  }

  public void setHiveConfig(DataWarehouseConfig hiveConfig) {
    this.hiveConfig = hiveConfig;
  }
  
  
}