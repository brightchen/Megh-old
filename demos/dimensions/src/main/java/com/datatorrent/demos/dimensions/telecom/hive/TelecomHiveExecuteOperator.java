package com.datatorrent.demos.dimensions.telecom.hive;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;
import com.datatorrent.contrib.hive.HiveOperator;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;

import jline.internal.Log;

public class TelecomHiveExecuteOperator extends HiveOperator
{
  private static final Logger logger = LoggerFactory.getLogger(TelecomHiveExecuteOperator.class);
  
  protected DataWarehouseConfig hiveConfig;
  
  protected String localString = "";
  protected String createTableSql;
  
  @Override
  public void setup(OperatorContext context)
  {
    try {
      //this class keep the url information, set to the store just to get rid of exception.
      store.setDatabaseUrl(getJdbcUrl());

      super.setup(context);

      checkIsHDFS();

      //syn tablename
      if (tablename == null || tablename.isEmpty())
        tablename = hiveConfig.getTableName();
      else
        hiveConfig.setTableName(tablename);
    } catch (IOException e) {
      logger.error("Got exception in setup.", e);
      throw new RuntimeException(e);
    }
  }
  
  protected boolean checkIsHDFS() throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(hivestore.filepath).toUri(), new Configuration());
    if (!tempFS.getScheme().equalsIgnoreCase("hdfs")) {
      localString = " local";
      return false;
    }
    return true;
  }
  
  protected void createBusinessTables()
  {
    if(createTableSql == null || createTableSql.isEmpty())
      return;

    try {
      logger.info("creating table using sql: ");
      logger.info(createTableSql);
      Statement stmt = getConnection().createStatement(); 
      stmt.execute(createTableSql);
      logger.info("table created.");
    }
    catch (SQLException ex) {
      logger.warn("create table failed. sql is '{}'; exception: {}", createTableSql, ex.getMessage());
    }
  }
  
  @Override
  public void processTuple(FilePartitionMapping tuple)
  {
    String command = getHiveCommand(tuple);
    logger.debug("command is {}",command);
    //should not put comma and the end of the sql
    //command = "load data local inpath '/Users/bright/src/bright/Megh/demos/dimensions/target/Hive.bak/hivedata.1' into table tempmap";
    if (command != null) {
      Statement stmt;
      try {
        //The HiveStore has problem with user.
        stmt = getConnection().createStatement(); 
        //stmt = hivestore.getConnection().createStatement();
        stmt.execute(command);
        
        String filePath = getFilePath(tuple);
        handleProcessedFile(filePath);
      }
      catch (SQLException ex) {
        logger.warn("Moving file into hive failed", ex);
      }
    }
  }
  
  protected void handleProcessedFile(String filePath)
  {
    //just remove it;
    try {
      fs.delete(new Path(filePath), true);
    } catch (IllegalArgumentException | IOException e) {
      logger.error("delete file exception. ", e);
    }
  }
  
  /**
   * The HiveStore has problem with user.
   */
  private Connection conn;
  protected Connection getConnection()
  {
    if(conn == null)
      try {
        conn = DriverManager.getConnection( getJdbcUrl(), hiveConfig.getUserName(), hiveConfig.getPassword());
      } catch (SQLException e) {
        logger.error("connection",e);
      }
    return conn;
  }
  
  protected String getJdbcUrl()
  {
    return HiveUtil.getUrl(hiveConfig.getHost(), hiveConfig.getPort(), hiveConfig.getDatabase());
  }
  
  protected boolean isAbsolutePath(String filePath)
  {
    return (filePath.length() > 0 && filePath.startsWith(File.separator));
  }
  
  protected String getFilePath(FilePartitionMapping tuple)
  {
    String filename = tuple.getFilename();
    ArrayList<String> partition = tuple.getPartition();
    String filepath = isAbsolutePath(filename) ? filename : hivestore.getFilepath() + Path.SEPARATOR + filename;
    logger.info("processing {} filepath", filepath);
    return filepath;
  }
  
  protected String getHiveCommand(FilePartitionMapping tuple)
  {
    String filepath = getFilePath(tuple);
    
    ArrayList<String> partition = tuple.getPartition();
    int numPartitions = partition.size();
    
    String command = null;
    try {
      if (fs.exists(new Path(filepath))) {
        if (numPartitions > 0) {
          StringBuilder partitionString = new StringBuilder(hivePartitionColumns.get(0) + "='" + partition.get(0) + "'");
          int i = 0;
          while (i < numPartitions) {
            i++;
            if (i == numPartitions) {
              break;
            }
            partitionString.append(",").append(hivePartitionColumns.get(i)).append("='").append(partition.get(i)).append("'");
          }
          if (i < hivePartitionColumns.size()) {
            partitionString.append(",").append(hivePartitionColumns.get(i));
          }
          command = "load data " + localString + " inpath '" + filepath + "' into table " + tablename + " PARTITION" + "( " + partitionString + " )";
        }
        else {
          command = "load data " + localString + " inpath '" + filepath + "' into table " + tablename;
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.info("command is {}" , command);
    return command;
  }

  public DataWarehouseConfig getHiveConfig()
  {
    return hiveConfig;
  }

  public void setHiveConfig(DataWarehouseConfig hiveConfig)
  {
    this.hiveConfig = hiveConfig;
  }

  public String getCreateTableSql()
  {
    return createTableSql;
  }

  public void setCreateTableSql(String createTableSql)
  {
    this.createTableSql = createTableSql;
  }
  
}
