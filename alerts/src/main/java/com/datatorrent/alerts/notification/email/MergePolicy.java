package com.datatorrent.alerts.notification.email;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public enum MergePolicy {
  
  appOnly( new Merger(){
    public String merge(String confValue, String appValue)
    {
      return appValue;
    }
    public Collection<String> merge(Collection<String> confValues, Collection<String> appValues)
    {
      return appValues;
    }
  }),
  
  configOnly(new Merger(){
    public String merge(String confValue, String appValue)
    {
      return confValue;
    }
    public Collection<String> merge(Collection<String> confValues, Collection<String> appValues)
    {
      return confValues;
    }
  }), 
  
  combine(new Merger(){
    public String merge(String confValue, String appValue)
    {
      return confValue + " " + appValue;
    }
    public Collection<String> merge(Collection<String> confValues, Collection<String> appValues)
    {
      Set<String> values = new HashSet<String>();
      values.addAll(confValues);
      values.addAll(appValues);
      return values;
    }
  }), 
  appOverConfig(new Merger(){
    public String merge(String confValue, String appValue)
    {
      return (appValue==null || appValue.isEmpty()) ? confValue : appValue;
    }
    public Collection<String> merge(Collection<String> confValues, Collection<String> appValues)
    {
      return (appValues==null || appValues.isEmpty()) ? confValues : appValues;
    }
  }), 
  configOverApp(new Merger(){
    public String merge(String confValue, String appValue)
    {
      return (confValue==null || confValue.isEmpty()) ? appValue : confValue;
    }
    public Collection<String> merge(Collection<String> confValues, Collection<String> appValues)
    {
      return (confValues==null || confValues.isEmpty()) ? appValues : confValues;
    }
  });
  
  private static interface Merger
  {
    public String merge(String confValue, String appValue);
    public Collection<String> merge(Collection<String> confValues, Collection<String> appValues);
  }

  private Merger merger;
  
  private MergePolicy( Merger merger )
  {
    this.merger = merger;
  }
  public String merge(String confValue, String appValue)
  {
    return merger.merge(confValue, appValue);
  }
  
  public Collection<String> merge(Collection<String> confValues, Collection<String> appValues)
  {
    return merger.merge(confValues, appValues);
  }
  
  public static MergePolicy fromValue(String value)
  {
    try
    {
      return MergePolicy.valueOf(value);
    }
    catch(Exception e)
    {
      return null;
    }
  }
}
