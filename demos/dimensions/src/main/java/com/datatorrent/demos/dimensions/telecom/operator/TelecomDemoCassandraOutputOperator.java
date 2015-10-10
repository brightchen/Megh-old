package com.datatorrent.demos.dimensions.telecom.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.cassandra.AbstractCassandraTransactionableOutputOperatorPS;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;

public abstract class TelecomDemoCassandraOutputOperator<T> extends AbstractCassandraTransactionableOutputOperatorPS<T>
{
  private static final transient Logger logger = LoggerFactory.getLogger(TelecomDemoCassandraOutputOperator.class);
  
  protected DataWarehouseConfig cassandraConfig;
  protected String sqlCommand;
  
  protected int batchSize = 100;
  
  @Override
  public void setup(OperatorContext context)
  {
    configure();
    createTables();
    createSqlFormat();
    super.setup(context);
  }
  
  protected abstract String createSqlFormat();
  
  protected void configure()
  {
    //store
    CassandraTransactionalStore store = new CassandraTransactionalStore();
    store.setNode(cassandraConfig.getHost());
    store.setKeyspace(cassandraConfig.getDatabase());

    setStore(store);
  }
  
  protected void createTables()
  {
    //this session just for create table
    Cluster cluster = Cluster.builder().addContactPoint(cassandraConfig.getHost()).build();
    Session session = cluster.connect(cassandraConfig.getDatabase());
    
    String createMetaTable = "CREATE TABLE IF NOT EXISTS " + CassandraTransactionalStore.DEFAULT_META_TABLE + " ( "
        + CassandraTransactionalStore.DEFAULT_APP_ID_COL + " TEXT, "
        + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT, "
        + CassandraTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT, "
        + "PRIMARY KEY (" + CassandraTransactionalStore.DEFAULT_APP_ID_COL + ", " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + ") "
        + ");";
    session.execute(createMetaTable);

    createBusinessTables(session);
    
    session.close();
  }
  
  protected abstract void createBusinessTables(Session session);
  
  @Override
  protected PreparedStatement getUpdateCommand()
  {
    return store.getSession().prepare(sqlCommand);
  }

  protected void commit()
  {
    //the commit was implemented by end window
    endWindow();
  }
  
  @Override
  public void endWindow()
  {
    try
    {
      super.endWindow();
      store.getBatchCommand().clear();
    }
    catch(Exception e )
    {
      logger.error(e.getMessage(), e);
    }
  }

  
  private int tupleCount = 0;
  @Override
  public void processTuple(T tuple)
  {
    super.processTuple(tuple);
    if(++tupleCount >= batchSize)
    {
      commit();
      tupleCount = 0;
    }
  }
}