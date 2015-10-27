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
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.telecom.conf.ConfigUtil;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCustomerService;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSingleSchemaDimensionStoreHDHTUpdateWithList;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSnapshotServerAggregate;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceEnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceGenerateOperator;
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
  public static final String SNAPSHOT_SCHEMA = "customerServiceDemoV2SnapshotSchema.json";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME
      + ".operator.Store.fileStore.basePathPrefix";
  
  public static final String PROP_CASSANDRA_HOST = "dt.application." + APP_NAME + ".cassandra.host";
  public static final String PROP_HBASE_HOST = "dt.application." + APP_NAME + ".hbase.host";
  public static final String PROP_HIVE_HOST = "dt.application." + APP_NAME + ".hive.host";
  
  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Cassandra = 0x100;
  
  protected int outputMask = outputMask_Cassandra;
  
  public String eventSchemaLocation = EVENT_SCHEMA;
  protected String snapshotSchemaLocation = SNAPSHOT_SCHEMA;

  protected boolean enableDimension = true;

  protected void populateConfig(Configuration conf)
  {
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
      dag.addOperator("HBasePersist", customerServicePersist);
      sustomerServiceStreamSinks.add(customerServicePersist.input);
    }
    if((outputMask & outputMask_Cassandra) != 0)
    {
      // Cassandra
      EnrichedCustomerServiceCassandraOutputOperator customerServicePersist = new EnrichedCustomerServiceCassandraOutputOperator();
      //dag.addOperator("CustomerService-Cassandra-Persist", customerServicePersist);
      dag.addOperator("CassandraPersist", customerServicePersist);
      sustomerServiceStreamSinks.add(customerServicePersist.input);
    }
    
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = null;
    if (enableDimension) {
      // dimension
      dimensions = dag.addOperator("DimensionsComputation",
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
        dimensions.setAggregateToExpression(aggregateToExpression);
      }

      // event schema
      dimensions.setConfigurationSchemaJSON(eventSchema);

      dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
      dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB,
          8092);

      // store
      AppDataSingleSchemaDimensionStoreHDHTUpdateWithList store = dag.addOperator("Store",
          AppDataSingleSchemaDimensionStoreHDHTUpdateWithList.class);
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
      store.setAggregatorID(AggregatorIncrementalType.COUNT.ordinal());
      store.setDimensionDescriptorID(6);
      //store.setDimensionalSchemaStubJSON(eventSchema);

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

      dag.addStream("DimensionalStream", dimensions.output, store.input);
      dag.addStream("QueryResult", store.queryResult, wsOut.input);
      
      //snapshot server
      AppDataSnapshotServerAggregate snapshotServer = new AppDataSnapshotServerAggregate();
      String snapshotServerJSON = SchemaUtils.jarResourceFileToString(snapshotSchemaLocation);
      snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
      snapshotServer.setEventSchema(eventSchema);
      {
        Map<MutablePair<String, Type>, MutablePair<String, Type>> keyValueMap = Maps.newHashMap();
        keyValueMap.put(new MutablePair<String, Type>("issueType", Type.STRING), new MutablePair<String, Type>("serviceCall", Type.LONG));
        snapshotServer.setKeyValueMap(keyValueMap);
      }
      dag.addOperator("SnapshotServer", snapshotServer);
      dag.addStream("Snapshot", store.updateWithList, snapshotServer.input);

      PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
      snapShotQuery.setUri(queryUri);
      //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
      snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);
      //dag.addStream("SnapshotQuery", snapShotQuery.outputPort, snapshotServer.query);
      
      
      PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
      snapShotQueryResult.setUri(queryUri);
      dag.addOperator("SnapshotQueryResult", snapShotQueryResult);
      dag.addStream("SnapshotQueryResult", snapshotServer.queryResult, snapShotQueryResult.input);
    }
    
    dag.addStream("EnrichedCustomerService", enrichOperator.outputPort, sustomerServiceStreamSinks.toArray(new DefaultInputPort[0]));
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
