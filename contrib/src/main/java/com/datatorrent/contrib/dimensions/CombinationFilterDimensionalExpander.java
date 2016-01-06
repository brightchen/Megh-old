package com.datatorrent.contrib.dimensions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

/**
 * This class verify the key value combination before adding to the result
 * @author bright
 *
 */
public class CombinationFilterDimensionalExpander extends SimpleDataQueryDimensionalExpander
{
  private static final Logger LOG = LoggerFactory.getLogger(CombinationFilterDimensionalExpander.class);
  
  protected CombinationFilter combinationFilter;
  
  public CombinationFilterDimensionalExpander(Map<String, Collection<Object>> seenEnumValues)
  {
    super(seenEnumValues);
  }

  public CombinationFilterDimensionalExpander withCombinationFilter(CombinationFilter combinationFilter)
  {
    this.setCombinationFilter(combinationFilter);
    return this;
  }

  protected void createKeyGPOsHelper(int index,
                                   Map<String, Set<Object>> keyToValues,
                                   FieldsDescriptor fd,
                                   List<String> fields,
                                   GPOMutable gpo,
                                   List<GPOMutable> resultGPOs)
  {
    if(index != 0)
      throw new IllegalArgumentException("it must start with index zero.");
    
    //set the value for empty set
    for(int i=0; i<fields.size(); ++i)
    {
      String key = fields.get(i);
      Set<Object> vals = keyToValues.get(key);
  
      if(vals.isEmpty()) {
        vals = Sets.newHashSet(seenKeyValues.get(key));
        keyToValues.put(key, vals);
      }
    }

    //cleanup
    if(combinationFilter != null)
      keyToValues = combinationFilter.filter(keyToValues);
    
    createKeyGPOsWithCleanKeyValues(index, keyToValues, fd, fields, gpo, resultGPOs);
  }
  
  protected void createKeyGPOsWithCleanKeyValues(int index,
      Map<String, Set<Object>> keyToValues,
      FieldsDescriptor fd,
      List<String> fields,
      GPOMutable gpo,
      List<GPOMutable> resultGPOs)
  {
    String key = fields.get(index);
    Set<Object> vals = keyToValues.get(key);
    
    for (Object val : vals) {
      GPOMutable gpoKey;

      if(index == 0) {
        gpoKey = new GPOMutable(fd);
      } else {
        gpoKey = new GPOMutable(gpo);
      }

      gpoKey.setFieldGeneric(key, val);
            
      if (index == fields.size() - 1) {
        resultGPOs.add(gpoKey);
      } else {
        createKeyGPOsWithCleanKeyValues(index + 1, keyToValues, fd, fields, gpoKey, resultGPOs);
      }
    }
  }

  public CombinationFilter getCombinationFilter()
  {
    return combinationFilter;
  }

  public void setCombinationFilter(CombinationFilter combinationFilter)
  {
    this.combinationFilter = combinationFilter;
  }

  
}
