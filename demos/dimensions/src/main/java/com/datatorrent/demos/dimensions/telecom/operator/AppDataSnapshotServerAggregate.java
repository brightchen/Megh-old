package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;

public class AppDataSnapshotServerAggregate extends AbstractAppDataSnapshotServer<Aggregate>
{
  @Override
  public GPOMutable convert(Aggregate aggregate)
  {
    return aggregate.getAggregates();
  }

}
