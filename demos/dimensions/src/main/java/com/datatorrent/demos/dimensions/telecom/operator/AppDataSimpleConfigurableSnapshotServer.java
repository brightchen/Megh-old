package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.Map;

import com.datatorrent.lib.appdata.gpo.GPOMutable;

/**
 * The tuple is a List of MutablePair<String, Long>
 * @author bright
 *
 */
public class AppDataSimpleConfigurableSnapshotServer extends AppDataConfigurableSnapshotServer<Map<String, Long>>
{

  @Override
  protected void convertTo(Map<String, Long> row, GPOMutable gpo)
  {
    for(Map.Entry<String, Long> entry : row.entrySet())
      gpo.setField(entry.getKey(), entry.getValue());
    
  }

}
