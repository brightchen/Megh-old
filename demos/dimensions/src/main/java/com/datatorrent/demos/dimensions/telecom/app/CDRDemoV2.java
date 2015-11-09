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
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.contrib.hive.HiveStore;
import com.datatorrent.demos.dimensions.telecom.conf.ConfigUtil;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHiveConfig;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.hive.TelecomHiveExecuteOperator;
import com.datatorrent.demos.dimensions.telecom.hive.TelecomHiveOutputOperator;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSnapshotServerAggregate;
import com.datatorrent.demos.dimensions.telecom.operator.CDREnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CDRStore;
import com.datatorrent.demos.dimensions.telecom.operator.CallDetailRecordGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRCassandraOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRHbaseOutputOperator;
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
 * Only need compute maximum Disconnects by Location (Latitude and Longitude)
 * 
 * @author bright
 *
 */
@ApplicationAnnotation(name = CDRDemoV2.APP_NAME)
public class CDRDemoV2 implements StreamingApplication {
  private static final transient Logger logger = LoggerFactory.getLogger(CDRDemoV2.class);

  public static final String APP_NAME = "CDRDemoV2";
  public static final String EVENT_SCHEMA = "cdrDemoV2EventSchema.json";
  public static final String SNAPSHOT_SCHEMA = "cdrDemoV2SnapshotSchema.json";
  
  

  public final String appName;
  protected String PROP_STORE_PATH;
  protected String PROP_CASSANDRA_HOST;
  protected String PROP_HBASE_HOST;
  protected String PROP_HIVE_HOST;
  protected String PROP_OUTPUT_MASK;
  protected String PROP_HIVE_TEMP_PATH;
  protected String PROP_HIVE_TEMP_FILE;
  
  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Hive = 0x02;
  public static final int outputMask_Cassandra = 0x04;
  
  protected int outputMask = outputMask_Cassandra;
  
  protected String eventSchemaLocation = EVENT_SCHEMA;
  protected String snapshotSchemaLocation = SNAPSHOT_SCHEMA;

  protected boolean enableDimension = true;
  //use absolute path or rename from tmp files will be failed due to different directory.
  protected String hiveTmpPath = "/user/cdrtmp";
  protected String hiveTmpFile = "cdr";
  protected String enrichedCDRTableSchema 
    = "CREATE TABLE IF NOT EXISTS %s ( isdn string, imsi string, imei string, plan string, callType string, correspType string, " +
      " correspIsdn string, duration string, bytes string, dr string, lat string, lon string, " +
      " drLable string, operatorCode string, deviceBrand string, deviceModel string, zipCode string )" + 
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";  


  public CDRDemoV2()
  {
    this(APP_NAME);
  }
  
  public CDRDemoV2(String appName)
  {
    this.appName = appName;
    PROP_CASSANDRA_HOST = "dt.application." + appName + ".cassandra.host";
    PROP_HBASE_HOST = "dt.application." + appName + ".hbase.host";
    PROP_HIVE_HOST = "dt.application." + appName + ".hive.host";
    
    PROP_STORE_PATH = "dt.application." + appName + ".operator.CDRStore.fileStore.basePathPrefix";
    PROP_OUTPUT_MASK = "dt.application." + appName + ".cdroutputmask";
    PROP_HIVE_TEMP_PATH = "dt.application." + appName + ".cdrhivetmppath";
    PROP_HIVE_TEMP_FILE = "dt.application." + appName + ".cdrhivetmpfile";
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
        
    {
      final String hiveTmpPath = conf.get(PROP_HIVE_TEMP_PATH);
      if(hiveTmpPath != null )
        this.hiveTmpPath = hiveTmpPath;
      logger.info("hiveTmpPath: {}", hiveTmpPath);
    }
    {
      final String hiveTmpFile = conf.get(PROP_HIVE_TEMP_FILE);
      if(hiveTmpFile != null )
        this.hiveTmpFile = hiveTmpFile;
      logger.info("hiveTmpFile: {}", hiveTmpFile);
    }
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    
    populateConfig(conf);
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);
    
    // CDR generator
    CallDetailRecordGenerateOperator cdrGenerator = new CallDetailRecordGenerateOperator();
    dag.addOperator("CDRGenerator", cdrGenerator);

    // CDR enrich
    CDREnrichOperator enrichOperator = new CDREnrichOperator();
    dag.addOperator("CDREnrich", enrichOperator);
    
    dag.addStream("InputStream", cdrGenerator.cdrOutputPort, enrichOperator.cdrInputPort)
    .setLocality(Locality.CONTAINER_LOCAL);

    List<DefaultInputPort<? super EnrichedCDR>> enrichedStreamSinks = Lists.newArrayList();
    // CDR persist
    if((outputMask & outputMask_HBase) != 0)
    {
      // HBase
      EnrichedCDRHbaseOutputOperator cdrPersist = new EnrichedCDRHbaseOutputOperator();
      dag.addOperator("CDRHBasePersist", cdrPersist);
      enrichedStreamSinks.add(cdrPersist.input);
    }
    if((outputMask & outputMask_Cassandra) != 0)
    {
      EnrichedCDRCassandraOutputOperator cdrPersist = new EnrichedCDRCassandraOutputOperator();
      dag.addOperator("CDRCanssandraPersist", cdrPersist);
      enrichedStreamSinks.add(cdrPersist.input);
    }
    if((outputMask & outputMask_Hive) != 0)
    {
      TelecomHiveOutputOperator hiveOutput = new TelecomHiveOutputOperator();
      if(hiveTmpPath != null)
        hiveOutput.setFilePath(hiveTmpPath);
      if(hiveTmpFile != null)
        hiveOutput.setOutputFileName(hiveTmpFile);
      hiveOutput.setMaxLength(1024*1024);
      hiveOutput.setFilePermission((short)511);

      dag.addOperator("CDRHiveOutput", hiveOutput);
      enrichedStreamSinks.add(hiveOutput.input);
      
      TelecomHiveExecuteOperator hiveExecute = new TelecomHiveExecuteOperator();

      {
        HiveStore hiveStore = new HiveStore();
        if(hiveTmpPath != null)
          hiveStore.setFilepath(hiveTmpPath);
        hiveExecute.setHivestore(hiveStore);
      }
      hiveExecute.setHiveConfig(EnrichedCDRHiveConfig.instance());
      String createTableSql = String.format( enrichedCDRTableSchema,  EnrichedCDRHiveConfig.instance().getDatabase() + "." + EnrichedCDRHiveConfig.instance().getTableName() );
      hiveExecute.setCreateTableSql(createTableSql);
      dag.addOperator("CDRHiveExecute", hiveExecute);
      dag.addStream("CDRHiveLoadData", hiveOutput.hiveCmdOutput, hiveExecute.input);
    }
    
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = null;
    if (enableDimension) {
      // dimension
      dimensions = dag.addOperator("CDRDimensionsComputation",
          DimensionsComputationFlexibleSingleSchemaPOJO.class);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);

      enrichedStreamSinks.add(dimensions.input);
      
      // Set operator properties
      // key expression: Point( Lat, Lon )
      {
        Map<String, String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("zipcode", "getZipCode()");
        keyToExpression.put("deviceModel", "getDeviceModel()");
        keyToExpression.put("time", "getTime()");
        dimensions.setKeyToExpression(keyToExpression);
      }

      // aggregate expression: disconnect
      {
        Map<String, String> aggregateToExpression = Maps.newHashMap();
        aggregateToExpression.put("disconnectCount", "getDisconnectCount()");
        aggregateToExpression.put("downloadBytes", "getBytes()");
        dimensions.setAggregateToExpression(aggregateToExpression);
      }

      // event schema
      dimensions.setConfigurationSchemaJSON(eventSchema);

      dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
      dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB,
          8092);

      // store
      CDRStore store = dag.addOperator("CDRStore", CDRStore.class);
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
      store.addAggregatorsInfo(AggregatorIncrementalType.SUM.ordinal(), 6);
      
      //should not setDimensionalSchemaStubJSON 
      //store.setDimensionalSchemaStubJSON(eventSchema);

      PubSubWebSocketAppDataQuery query = createAppDataQuery();
      URI queryUri = ConfigUtil.getAppDataQueryPubSubURI(dag, conf);
      logger.info("QueryUri: {}", queryUri);
      query.setUri(queryUri);
      store.setEmbeddableQueryInfoProvider(query);
      //enable partition after Tim merge the fixing
//      store.setPartitionCount(4);
//      store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());
      
      // wsOut
      PubSubWebSocketAppDataResult wsOut = createAppDataResult();
      wsOut.setUri(queryUri);
      dag.addOperator("CDRQueryResult", wsOut);
      // Set remaining dag options

      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
          new BasicCounters.LongAggregator<MutableLong>());

      dag.addStream("CDRDimensionalStream", dimensions.output, store.input);
      dag.addStream("CDRQueryResult", store.queryResult, wsOut.input);
      
      
      //snapshot server
      AppDataSnapshotServerAggregate snapshotServer = new AppDataSnapshotServerAggregate();
      String snapshotServerJSON = SchemaUtils.jarResourceFileToString(snapshotSchemaLocation);
      snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
      snapshotServer.setEventSchema(eventSchema);
      {
        Map<MutablePair<String, Type>, MutablePair<String, Type>> keyValueMap = Maps.newHashMap();
        keyValueMap.put(new MutablePair<String, Type>("deviceModel", Type.STRING), new MutablePair<String, Type>("downloadBytes", Type.LONG));
        snapshotServer.setKeyValueMap(keyValueMap);
      }
      dag.addOperator("BandwidthServer", snapshotServer);
      dag.addStream("Bandwidth", store.updateWithList, snapshotServer.input);

      PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
      snapShotQuery.setUri(queryUri);
      //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
      snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);
      //dag.addStream("SnapshotQuery", snapShotQuery.outputPort, snapshotServer.query);
      
      
      PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
      snapShotQueryResult.setUri(queryUri);
      dag.addOperator("BandwidthQueryResult", snapShotQueryResult);
      dag.addStream("BandwidthQueryResult", snapshotServer.queryResult, snapShotQueryResult.input);
      
    }
    dag.addStream("CDREnriched", enrichOperator.outputPort, enrichedStreamSinks.toArray(new DefaultInputPort[0]));
      
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

  public String getEventSchemaLocation()
  {
    return eventSchemaLocation;
  }

  public void setEventSchemaLocation(String eventSchemaLocation)
  {
    this.eventSchemaLocation = eventSchemaLocation;
  }

  public String getSnapshotSchemaLocation()
  {
    return snapshotSchemaLocation;
  }

  public void setSnapshotSchemaLocation(String snapshotSchemaLocation)
  {
    this.snapshotSchemaLocation = snapshotSchemaLocation;
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
