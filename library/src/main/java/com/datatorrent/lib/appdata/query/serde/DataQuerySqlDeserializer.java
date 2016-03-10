/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsAggregatable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * This class could cover the snapshot, as snapshot only without bucket information
 * 
 */
public class DataQuerySqlDeserializer implements CustomMessageDeserializer
{
  protected DataQueryDimensionalInfoProvider dataQueryDimensionalInfoProvider;
  protected long countdown = 1;
  protected boolean incompleteResultOK = true;
  protected boolean oneTime = true;
  protected long id;    //which should get from the request
  protected final String type = "dataQuery";
  protected Map<String, String> schemaKeys = null;
  /**
   * select location, sum(impression) where publisher=‘google’ order by sum(impression) asc|desc group by location limit 10
   */
  @Override
  public Message deserialize(String sql, Class<? extends Message> message, Object context) throws IOException
  {
    SchemaRegistry schemaRegistry = ((SchemaRegistry)context);
    DimensionalSchema gsd = (DimensionalSchema)schemaRegistry.getSchema(schemaKeys);

    boolean hasFromTo = false;
    int latestNumBuckets = -1;
    long from = 0;
    long to = 0;
    CustomTimeBucket bucket = null;


    ////keys
    Map<String, Set<Object>> keyFieldToValues = dataQueryDimensionalInfoProvider.getKeyFieldToValues();
    final Set<String> keyFieldSet = keyFieldToValues.keySet();
    DimensionsDescriptor dimensionDescriptor = new DimensionsDescriptor(bucket, new Fields(keyFieldSet));
    Integer ddID = gsd.getDimensionalConfigurationSchema().getDimensionsDescriptorToID().get(dimensionDescriptor);
    if (ddID == null) {
      LOG.error("The given dimensionDescriptor is not valid: {}", dimensionDescriptor);
      return null;
    }

    Map<String, Set<String>> valueToAggregator =
        gsd.getDimensionalConfigurationSchema().getDimensionsDescriptorIDToValueToAggregator().get(ddID);

    ////Fields
    Map<String, Set<String>> queryValueFieldToAggregator = dataQueryDimensionalInfoProvider.getValueFieldToAggregators();
    Set<String> nonAggregatedFields = dataQueryDimensionalInfoProvider.getNonAggregatedValueFields();

    FieldsAggregatable queryFields = new FieldsAggregatable(nonAggregatedFields, queryValueFieldToAggregator);
    FieldsDescriptor keyFieldsDescriptor = gsd.getDimensionalConfigurationSchema().getKeyDescriptor().getSubset(new Fields(keyFieldSet));
    DataQueryDimensional resultQuery = new DataQueryDimensional(
              String.valueOf(id),
              type,
              latestNumBuckets,
              bucket,
              keyFieldsDescriptor,
              null,   //keyFieldToValues,
              queryFields,
              incompleteResultOK,
              schemaKeys);

    return resultQuery;
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(DataQueryDimensionalDeserializer.class);
}
