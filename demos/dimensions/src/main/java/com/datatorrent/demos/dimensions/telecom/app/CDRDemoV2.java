package com.datatorrent.demos.dimensions.telecom.app;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
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
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;
import com.datatorrent.demos.dimensions.telecom.operator.CDREnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CallDetailRecordGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRCassandraOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRHbaseOutputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
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
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME
      + ".operator.Store.fileStore.basePathPrefix";
  
  public static final String PROP_CASSANDRA_HOST = "dt.application." + APP_NAME + ".cassandra.host";
  public static final String PROP_HBASE_HOST = "dt.application." + APP_NAME + ".hbase.host";
  public static final String PROP_HIVE_HOST = "dt.application." + APP_NAME + ".hive.host";
  
  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Cassandra = 0x100;
  
  protected int outputMask = outputMask_Cassandra;
  
  public String eventSchemaLocation = EVENT_SCHEMA;

  protected boolean enableDimension = true;

  protected void populateConfig(Configuration conf)
  {
    {
      final String cassandraHost = conf.get(PROP_CASSANDRA_HOST);
      if(cassandraHost != null)
      {
        TelecomDemoConf.instance.setCassandraHost(cassandraHost);
      }
    }
    
    {
      final String hbaseHost = conf.get(PROP_HBASE_HOST);
      if(hbaseHost != null)
      {
        TelecomDemoConf.instance.setHbaseHost(hbaseHost);
      }
    }
    
    {
      final String hiveHost = conf.get(PROP_HIVE_HOST);
      if(hiveHost != null)
      {
        TelecomDemoConf.instance.setHbaseHost(hiveHost);
      }
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
      dag.addOperator("EnrichedCDR-HBase-Persist", cdrPersist);
      enrichedStreamSinks.add(cdrPersist.input);
    }
    if((outputMask & outputMask_Cassandra) != 0)
    {
      EnrichedCDRCassandraOutputOperator cdrPersist = new EnrichedCDRCassandraOutputOperator();
      dag.addOperator("EnrichedCDR-Canssandra-Persist", cdrPersist);
      enrichedStreamSinks.add(cdrPersist.input);
    }
    
    
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = null;
    if (enableDimension) {
      // dimension
      dimensions = dag.addOperator("DimensionsComputation",
          DimensionsComputationFlexibleSingleSchemaPOJO.class);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);

      enrichedStreamSinks.add(dimensions.input);
      
      // Set operator properties
      // key expression: Point( Lat, Lon )
      {
        Map<String, String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("zipcode", "getZipCode()");
        keyToExpression.put("time", "getTime()");
        dimensions.setKeyToExpression(keyToExpression);
      }

      // aggregate expression: disconnect
      {
        Map<String, String> aggregateToExpression = Maps.newHashMap();
        aggregateToExpression.put("disconnectCount", "getDisconnectCount()");
        dimensions.setAggregateToExpression(aggregateToExpression);
      }

      // event schema
      dimensions.setConfigurationSchemaJSON(eventSchema);

      dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
      dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB,
          8092);

      // store
      AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store",
          AppDataSingleSchemaDimensionStoreHDHT.class);
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
      //should not setDimensionalSchemaStubJSON 
      //store.setDimensionalSchemaStubJSON(eventSchema);

      PubSubWebSocketAppDataQuery query = createAppDataQuery();
      store.setEmbeddableQueryInfoProvider(query);

      // wsOut
      PubSubWebSocketAppDataResult wsOut = createAppDataResult();
      dag.addOperator("QueryResult", wsOut);
      // Set remaining dag options

      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
          new BasicCounters.LongAggregator<MutableLong>());

      dag.addStream("DimensionalStream", dimensions.output, store.input);
      dag.addStream("QueryResult", store.queryResult, wsOut.input);
    }
    dag.addStream("EnrichedStream", enrichOperator.outputPort, enrichedStreamSinks.toArray(new DefaultInputPort[0]));
      
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

}
