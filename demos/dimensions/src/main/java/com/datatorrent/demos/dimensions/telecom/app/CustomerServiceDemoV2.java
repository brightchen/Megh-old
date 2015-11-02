package com.datatorrent.demos.dimensions.telecom.app;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.telecom.conf.ConfigUtil;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCustomerService;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSimpleConfigurableSnapshotServer;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSnapshotServerAggregate;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceEnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceStore;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCustomerServiceCassandraOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCustomerServiceHbaseOutputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

/**
 * 
 * # of service calls by Zipcode
 * Top 10 Zipcodes by Service Calls -> Drill Down to get Customer records
 * # Total wait time v/s Average Wait time for Top 10 Zipcodes
 * I also want running wait times for all zipcodes
 *
 * @author bright
 *
 */
@ApplicationAnnotation(name = CustomerServiceDemoV2.APP_NAME)
public class CustomerServiceDemoV2 implements StreamingApplication {
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceDemoV2.class);
  
  public static final String APP_NAME = "CustomerServiceDemoV2";
  public static final String EVENT_SCHEMA = "customerServiceDemoV2EventSchema.json";
  public static final String SERVICE_CALL_SCHEMA = "serviceCallSnapshotSchema.json";
  public static final String SATISFACTION_RATING_SCHEMA = "satisfactionRatingSnapshotSchema.json";
  public static final String AVERAGE_WAITTIME_SCHEMA = "averageWaittimeSnapshotSchema.json";
  
  public final String appName;
  protected String PROP_STORE_PATH;
  protected String PROP_CASSANDRA_HOST;
  protected String PROP_HBASE_HOST;
  protected String PROP_HIVE_HOST;
  protected String PROP_OUTPUT_MASK;
  
  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Cassandra = 0x100;
  
  protected int outputMask = outputMask_Cassandra;
  
  public String eventSchemaLocation = EVENT_SCHEMA;
  protected String serviceCallSchemaLocation = SERVICE_CALL_SCHEMA;
  protected String satisfactionRatingSchemaLocation = SATISFACTION_RATING_SCHEMA;
  protected String averageWaittimeSchemaLocation = AVERAGE_WAITTIME_SCHEMA;
  
  protected boolean enableDimension = true;


  public CustomerServiceDemoV2()
  {
    this(APP_NAME);
  }
  
  public CustomerServiceDemoV2(String appName)
  {
    this.appName = appName;
    PROP_CASSANDRA_HOST = "dt.application." + appName + ".cassandra.host";
    PROP_HBASE_HOST = "dt.application." + appName + ".hbase.host";
    PROP_HIVE_HOST = "dt.application." + appName + ".hive.host";
    PROP_STORE_PATH = "dt.application." + appName + ".operator.CSStore.fileStore.basePathPrefix";
    PROP_OUTPUT_MASK = "dt.application." + appName + ".csoutputmask";
  }
  
  protected void populateConfig(Configuration conf)
  {
    {
      final String sOutputMask = conf.get(PROP_OUTPUT_MASK);
      if(sOutputMask != null)
      {
        try
        {
          outputMask = Integer.valueOf(sOutputMask);
          logger.info("outputMask: {}", outputMask);
        }
        catch(Exception e)
        {
          logger.error("Invalid outputmask: {}", sOutputMask);
        }
      }
      
    }
    {
      final String cassandraHost = conf.get(PROP_CASSANDRA_HOST);
      if(cassandraHost != null)
      {
        TelecomDemoConf.instance.setCassandraHost(cassandraHost);
      }
      logger.info("CassandraHost: {}", TelecomDemoConf.instance.getCassandraHost());
    }
    
    {
      final String hbaseHost = conf.get(PROP_HBASE_HOST);
      if(hbaseHost != null)
      {
        TelecomDemoConf.instance.setHbaseHost(hbaseHost);
      }
      logger.info("HbaseHost: {}", TelecomDemoConf.instance.getHbaseHost());
    }
    
    {
      final String hiveHost = conf.get(PROP_HIVE_HOST);
      if(hiveHost != null)
      {
        TelecomDemoConf.instance.setHiveHost(hiveHost);
      }
      logger.info("HiveHost: {}", TelecomDemoConf.instance.getHiveHost());
    }
        
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    populateConfig(conf);
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    // Customer service generator
    CustomerServiceGenerateOperator customerServiceGenerator = new CustomerServiceGenerateOperator();
    dag.addOperator("CustomerServiceGenerator", customerServiceGenerator);
    
    CustomerServiceEnrichOperator enrichOperator = new CustomerServiceEnrichOperator();
    dag.addOperator("Enrich", enrichOperator);
    
    dag.addStream("CustomerService", customerServiceGenerator.outputPort, enrichOperator.inputPort);

    List<DefaultInputPort<? super EnrichedCustomerService>> sustomerServiceStreamSinks = Lists.newArrayList();
    
    // Customer service persist
    if((outputMask & outputMask_HBase) != 0)
    {
      // HBase
      EnrichedCustomerServiceHbaseOutputOperator customerServicePersist = new EnrichedCustomerServiceHbaseOutputOperator();
      dag.addOperator("CSHBasePersist", customerServicePersist);
      sustomerServiceStreamSinks.add(customerServicePersist.input);
    }
    if((outputMask & outputMask_Cassandra) != 0)
    {
      // Cassandra
      EnrichedCustomerServiceCassandraOutputOperator customerServicePersist = new EnrichedCustomerServiceCassandraOutputOperator();
      //dag.addOperator("CustomerService-Cassandra-Persist", customerServicePersist);
      dag.addOperator("CSCassandraPersist", customerServicePersist);
      sustomerServiceStreamSinks.add(customerServicePersist.input);
    }
    
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = null;
    if (enableDimension) {
      // dimension
      dimensions = dag.addOperator("CSDimensionsComputation",
          DimensionsComputationFlexibleSingleSchemaPOJO.class);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);
      sustomerServiceStreamSinks.add(dimensions.input);
      
      // Set operator properties
      // key expression
      {
        Map<String, String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("zipCode", "getZipCode()");
        keyToExpression.put("issueType", "getIssueType()");
        keyToExpression.put("time", "getTime()");
        dimensions.setKeyToExpression(keyToExpression);
      }

      // aggregate expression
      {
        Map<String, String> aggregateToExpression = Maps.newHashMap();
        aggregateToExpression.put("serviceCall", "getServiceCallCount()");
        aggregateToExpression.put("wait", "getWait()");
        aggregateToExpression.put("satisfaction", "getSatisfaction()");
        dimensions.setAggregateToExpression(aggregateToExpression);
      }

      // event schema
      dimensions.setConfigurationSchemaJSON(eventSchema);

      dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
      dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB,
          8092);

      // store
      CustomerServiceStore store = dag.addOperator("CSStore", CustomerServiceStore.class);
      String basePath = conf.get(PROP_STORE_PATH);
      if (basePath == null || basePath.isEmpty())
        basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
            "base path should be specified in the properties.xml");
      TFileImpl hdsFile = new TFileImpl.DTFileImpl();
      basePath += System.currentTimeMillis();
      hdsFile.setBasePath(basePath);

      store.setFileStore(hdsFile);
      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
          new BasicCounters.LongAggregator<MutableLong>());
      store.setConfigurationSchemaJSON(eventSchema);
      //for bandwidth usage
      store.addAggregatorsInfo(AggregatorIncrementalType.COUNT.ordinal(), 6);

      PubSubWebSocketAppDataQuery query = createAppDataQuery();
      URI queryUri = ConfigUtil.getAppDataQueryPubSubURI(dag, conf);
      logger.info("QueryUri: {}", queryUri);
      query.setUri(queryUri);
      store.setEmbeddableQueryInfoProvider(query);

      // wsOut
      PubSubWebSocketAppDataResult wsOut = createAppDataResult();
      wsOut.setUri(queryUri);
      dag.addOperator("QueryResult", wsOut);
      // Set remaining dag options

      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
          new BasicCounters.LongAggregator<MutableLong>());

      dag.addStream("CSDimensionalStream", dimensions.output, store.input);
      dag.addStream("CSQueryResult", store.queryResult, wsOut.input);
      
      //snapshot servers
      //ServiceCall
      {
        AppDataSnapshotServerAggregate snapshotServer = new AppDataSnapshotServerAggregate();
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(serviceCallSchemaLocation);
        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.setEventSchema(eventSchema);
        {
          Map<MutablePair<String, Type>, MutablePair<String, Type>> keyValueMap = Maps.newHashMap();
          keyValueMap.put(new MutablePair<String, Type>("issueType", Type.STRING), new MutablePair<String, Type>("serviceCall", Type.LONG));
          snapshotServer.setKeyValueMap(keyValueMap);
        }
        dag.addOperator("ServiceCallServer", snapshotServer);
        dag.addStream("ServiceCallSnapshot", store.serviceCallOutputPort, snapshotServer.input);
  
        PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
        snapShotQuery.setUri(queryUri);
        //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
        snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);
        //dag.addStream("SnapshotQuery", snapShotQuery.outputPort, snapshotServer.query);
        
        
        PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
        snapShotQueryResult.setUri(queryUri);
        dag.addOperator("ServiceCallQueryResult", snapShotQueryResult);
        dag.addStream("ServiceCallResult", snapshotServer.queryResult, snapShotQueryResult.input);
      }
      
      //satisfaction rating
      {
        AppDataSimpleConfigurableSnapshotServer snapshotServer = new AppDataSimpleConfigurableSnapshotServer();
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(this.satisfactionRatingSchemaLocation);
        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.addStaticFieldInfo("min", 0L);
        snapshotServer.addStaticFieldInfo("max", 100L);
        snapshotServer.addStaticFieldInfo("barrier", 80L);
        //snapshotServer.setEventSchema(eventSchema);
        {
          Map<String, Type> fieldInfo = Maps.newHashMap();
          fieldInfo.put("satisfaction", Type.LONG);
          fieldInfo.put("min", Type.LONG);
          fieldInfo.put("max", Type.LONG);
          fieldInfo.put("barrier", Type.LONG);
          snapshotServer.setFieldInfoMap(fieldInfo);
        }
        dag.addOperator("SatisfactionServer", snapshotServer);
        dag.addStream("Satisfaction", store.satisfactionRatingOutputPort, snapshotServer.input);
  
        PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
        snapShotQuery.setUri(queryUri);
        //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
        snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);
        //dag.addStream("SnapshotQuery", snapShotQuery.outputPort, snapshotServer.query);
        
        
        PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
        snapShotQueryResult.setUri(queryUri);
        dag.addOperator("SatisfactionQueryResult", snapShotQueryResult);
        dag.addStream("SatisfactionQueryResult", snapshotServer.queryResult, snapShotQueryResult.input);
      }

    
      //Wait time
      {
        AppDataSimpleConfigurableSnapshotServer snapshotServer = new AppDataSimpleConfigurableSnapshotServer();
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(this.averageWaittimeSchemaLocation);
        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.addStaticFieldInfo("min", 0L);
        snapshotServer.addStaticFieldInfo("max", 200L);
        snapshotServer.addStaticFieldInfo("barrier", 30L);
        //snapshotServer.setEventSchema(eventSchema);
        {
          Map<String, Type> fieldInfo = Maps.newHashMap();
          fieldInfo.put("wait", Type.LONG);
          fieldInfo.put("min", Type.LONG);
          fieldInfo.put("max", Type.LONG);
          fieldInfo.put("barrier", Type.LONG);
          snapshotServer.setFieldInfoMap(fieldInfo);
        }
        dag.addOperator("WaittimeServer", snapshotServer);
        dag.addStream("Waittime", store.averageWaitTimeOutputPort, snapshotServer.input);
  
        PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
        snapShotQuery.setUri(queryUri);
        //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
        snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);
        //dag.addStream("SnapshotQuery", snapShotQuery.outputPort, snapshotServer.query);
        
        
        PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
        snapShotQueryResult.setUri(queryUri);
        dag.addOperator("WaittimeQueryResult", snapShotQueryResult);
        dag.addStream("WaittimeQueryResult", snapshotServer.queryResult, snapShotQueryResult.input);
      }
    }
  
    dag.addStream("CSEnriched", enrichOperator.outputPort, sustomerServiceStreamSinks.toArray(new DefaultInputPort[0]));
  }

  public boolean isEnableDimension() {
    return enableDimension;
  }

  public void setEnableDimension(boolean enableDimension) {
    this.enableDimension = enableDimension;
  }

  protected PubSubWebSocketAppDataQuery createAppDataQuery() {
    return new PubSubWebSocketAppDataQuery();
  }

  protected PubSubWebSocketAppDataResult createAppDataResult() {
    return new PubSubWebSocketAppDataResult();
  }
  
  public int getOutputMask()
  {
    return outputMask;
  }
  public void setOutputMask(int outputMask)
  {
    this.outputMask = outputMask;
  }

}
