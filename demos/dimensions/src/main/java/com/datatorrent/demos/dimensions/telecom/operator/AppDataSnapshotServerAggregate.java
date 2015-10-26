package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

public class AppDataSnapshotServerAggregate extends AbstractAppDataSnapshotServer<Aggregate>
{
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
    aggregate.getAggregates().setFieldDescriptor(dimensitionSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(aggregate.getDimensionDescriptorID()).get(aggregate.getAggregatorID()));
    
    return aggregate.getAggregates();
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
