package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

public class AppDataSnapshotServerAggregate extends AbstractAppDataSnapshotServer<Aggregate>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AppDataSnapshotServerAggregate.class);
      
  private String eventSchema;
  private transient DimensionalConfigurationSchema dimensitionSchema;
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
    dimensitionSchema = new DimensionalConfigurationSchema(eventSchema, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
  }
  
  @Override
  public GPOMutable convert(Aggregate aggregate)
  {
    final FieldsDescriptor aggregatesFd = dimensitionSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(aggregate.getDimensionDescriptorID()).get(aggregate.getAggregatorID());
    aggregate.getAggregates().setFieldDescriptor(aggregatesFd);
    
    final FieldsDescriptor keysFd = dimensitionSchema.getDimensionsDescriptorIDToKeyDescriptor().get(aggregate.getDimensionDescriptorID());
    GPOMutable keys = aggregate.getKeys();
    keys.setFieldDescriptor(keysFd);
    
    {
      Type fieldType = keysFd.getType("deviceModel");
      logger.info("The type of field '{}' is '{}'", "deviceModel", fieldType);
    }
    
    String deviceModel = keys.getFieldString("deviceModel");
    
    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("deviceModel", Type.STRING);
    fieldToType.put("downloadBytes", Type.LONG);
    
    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);
    
    GPOMutable gpo = new GPOMutable(fd);
    gpo.setField("deviceModel", deviceModel);
    gpo.setField("downloadBytes", aggregate.getAggregates().getFieldLong("downloadBytes"));
    
    return gpo;
    
  }

  public String getEventSchema()
  {
    return eventSchema;
  }

  public void setEventSchema(String eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  
}
