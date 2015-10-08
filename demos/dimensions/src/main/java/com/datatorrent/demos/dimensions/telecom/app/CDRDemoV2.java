package com.datatorrent.demos.dimensions.telecom.app;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;
import com.datatorrent.demos.dimensions.telecom.operator.CDREnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CallDetailRecordGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceHbaseOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRHbaseOutputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Only need compute maximum Disconnects by Location (Latitude and Longitude)
 * 
 * @author bright
 *
 */
@ApplicationAnnotation(name = CDRDemoV2.APP_NAME)
public class CDRDemoV2 implements StreamingApplication {

  public static final String APP_NAME = "CDRDemoV2";
  public static final String EVENT_SCHEMA = "cdrDemoV2EventSchema.json";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME
      + ".operator.Store.fileStore.basePathPrefix";

  public String eventSchemaLocation = EVENT_SCHEMA;

  protected boolean enableDimension = true;
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    // CDR generator
    CallDetailRecordGenerateOperator cdrGenerator = new CallDetailRecordGenerateOperator();
    dag.addOperator("CDR-Generator", cdrGenerator);

    // CDR enrich
    CDREnrichOperator enrichOperator = new CDREnrichOperator();
    dag.addOperator("CDR-Enrich", enrichOperator);

    // CDR persist
    EnrichedCDRHbaseOutputOperator cdrPersist = new EnrichedCDRHbaseOutputOperator();
    dag.addOperator("EnrichedCDR-Persist", cdrPersist);

    dag.addStream("InputStream", cdrGenerator.cdrOutputPort, enrichOperator.cdrInputPort)
        .setLocality(Locality.CONTAINER_LOCAL);
    
    
    // Customer service generator
    CustomerServiceGenerateOperator customerServiceGenerator = new CustomerServiceGenerateOperator();
    dag.addOperator("CustomerService-Generator", customerServiceGenerator);
    
    // Customer service persist
    CustomerServiceHbaseOutputOperator customerServicePersist = new CustomerServiceHbaseOutputOperator();
    dag.addOperator("CustomerService-Persist", customerServicePersist);
    
    dag.addStream("CustomerService", customerServiceGenerator.outputPort, customerServicePersist.input);
    
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = null;
    if (enableDimension) {
      // dimension
      dimensions = dag.addOperator("DimensionsComputation",
          DimensionsComputationFlexibleSingleSchemaPOJO.class);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);

      // Set operator properties
      // key expression: Point( Lat, Lon )
      {
        Map<String, String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("point", "getPoint()");
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
    if(dimensions != null)
      dag.addStream("EnrichedStream", enrichOperator.outputPort, cdrPersist.input, dimensions.input);
    else
      dag.addStream("EnrichedStream", enrichOperator.outputPort, cdrPersist.input);
      
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
