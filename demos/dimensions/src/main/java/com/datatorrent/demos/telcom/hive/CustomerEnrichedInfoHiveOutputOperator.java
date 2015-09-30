package com.datatorrent.demos.telcom.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.telcom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoHiveOutputOperator extends BaseOperator{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoHiveOutputOperator.class);
  
  private String host = "localhost";
  private int port = 10000;
  private String userName;
  private String password;
  private String database = "telcomdemo";
  private String tableName = "CustomerEnrichedInfo";
  private boolean startOver = false;
  
  protected Connection connect;
  
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
  public void setup(OperatorContext context)
  {
    //create table;
    try
    {
      createTable();
    }
    catch(Exception e)
    {
      logger.error("create table '{}' failed.\n exception: {}", tableName, e.getMessage());
    }
  }
  
  protected Connection getConnect() throws SQLException, ClassNotFoundException
  {
    HiveUtil.verifyDriver();
    
    if(connect == null)
    {
      String url = HiveUtil.getUrl(host, port, database);
      connect = DriverManager.getConnection(url, userName, password);    
    }
    return connect;
  }

  protected void createTable() throws Exception
  {
    Connection connect = getConnect();
    Statement stmt = connect.createStatement();
    
    ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'");
    boolean hasTable = rs.first();
    
    if(hasTable && startOver)
    {
      stmt.executeUpdate("drop table " + tableName);
      hasTable = false;
    }
    
    if(!hasTable)
    {
      //create table;
      String tableSchema = "(id long, imsi string, isdn string, imei string, operatorCode string, operatorName string, deviceBrand string, deviceModel string)";
      stmt.executeUpdate("create table " + tableName + tableSchema);
    }
    stmt.close();
  }
  
  private Statement insertStatement;
  protected Statement getInsertStatement() throws ClassNotFoundException, SQLException
  {
    if(insertStatement == null)
      insertStatement = getConnect().createStatement();
    return insertStatement;
  }

  private int batchCount = 1000;
  private int batchSize = 0;
  public void processTuple(SingleRecord tuple)
  {
    final String sqlValueFormat = "(%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
    String sql = "insert into table " + tableName + " values " 
        + String.format( sqlValueFormat, tuple.id, tuple.imsi, tuple.isdn, tuple.imei, tuple.operatorCode, tuple.operatorName, tuple.deviceBrand, tuple.deviceModel );
    
    try {
      getInsertStatement().addBatch(sql);
      ++batchSize;
      
      if(batchSize >= batchCount)
        insertStatement.executeBatch();
      
      batchSize = 0;
      
    } catch (ClassNotFoundException | SQLException e) {
      logger.error(e.getMessage(), e);
    }
  }
  
  @Override
  public void endWindow()
  {
    try
    {
      if(batchSize > 0)
        insertStatement.executeBatch();
    }
    catch(SQLException e)
    {
      logger.error(e.getMessage(), e);
    }
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  
}
