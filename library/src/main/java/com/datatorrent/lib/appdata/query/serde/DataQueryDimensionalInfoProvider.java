/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.util.Map;
import java.util.Set;

/**
 * provide an abstract layer of presentation(json, sql etc) 
 *
 */
public interface DataQueryDimensionalInfoProvider
{
  public Set<String> getNonAggregatedValueFields();
  public Map<String, Set<String>> getValueFieldToAggregators();
  
  /*
   * get the key field to it's values, the relation of values are or
   * basically it is criterias. only support equals
   */
  public Map<String, Set<Object>> getKeyFieldToValues();
}
