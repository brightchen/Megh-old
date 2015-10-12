package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.cassandra.CassandraStore;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;

public abstract class TelecomDemoCassandraOutputOperator<T> extends BaseOperator
{
  private static final transient Logger logger = LoggerFactory.getLogger(TelecomDemoCassandraOutputOperator.class);
  
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
      {
        @Override
        public void process(T tuple)
        {
          processTuple(tuple);
        }
      };
  
  
  protected DataWarehouseConfig cassandraConfig;
  protected String sqlCommand;
  
  protected int batchSize = 1000;
  protected List<T> tuples = Lists.newArrayList();
  protected CassandraStore store;
  protected Session session;
  
  @Override
  public void setup(OperatorContext context)
  {
    configure();
    createSession();
    createTables();
    createSqlFormat();
  }
  
  protected abstract String createSqlFormat();
  
  protected void configure()
  {
    //store
    store = new CassandraStore();
    store.setNode(cassandraConfig.getHost());
    store.setKeyspace(cassandraConfig.getDatabase());
  }
  
  protected void createSession()
  {
    Cluster cluster = Cluster.builder().addContactPoint(cassandraConfig.getHost()).build();
    session = cluster.connect(cassandraConfig.getDatabase());
  }
  
  protected void createTables()
  {
    createBusinessTables(session);
  }
  
  protected abstract void createBusinessTables(Session session);
  
  //@Override
  protected PreparedStatement prepareStatement()
  {
    return session.prepare(sqlCommand);
  }
  protected abstract Statement setStatementParameters(PreparedStatement updateCommand, T tuple) throws DriverException;
  
  protected int size = 0;
  protected PreparedStatement preparedStatement;
  public void processTuple(T tuple)
  {
    if(size >= batchSize)
      size = 0;
    if(size == 0)
      preparedStatement = prepareStatement();
   
    Statement statement = setStatementParameters(preparedStatement, tuple);
    session.execute(statement);
  }
}