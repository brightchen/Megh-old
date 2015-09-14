package com.datatorrent.alerts.action.email;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

public class EmailRecipient implements MergePolicySupported {
  protected final Collection<String> tos;
  protected final Collection<String> ccs;
  protected final Collection<String> bccs;
  protected final MergePolicy mergePolicy;
  
  public EmailRecipient( Collection<String> tos, Collection<String> ccs, Collection<String> bccs, String mergePolicy )
  {
    this( tos, ccs, bccs, MergePolicy.fromValue(mergePolicy) );
  }
  public EmailRecipient( Collection<String> tos, Collection<String> ccs, Collection<String> bccs, MergePolicy mergePolicy )
  {
    this.tos = Collections.unmodifiableCollection(tos);
    this.ccs = Collections.unmodifiableCollection(ccs);
    this.bccs = Collections.unmodifiableCollection(bccs);
    this.mergePolicy = mergePolicy;
  }
  
  @Override
  public String toString()
  {
    return String.format("tos: %s; ccs: %s; bccs: %s; mergePolicy: %s", toString(tos), toString(ccs), toString(bccs), mergePolicy);
  }

  public static String toString( Collection<String> collection)
  {
    if(collection == null)
      return "null";

    StringBuilder sb = new StringBuilder();
    for(String item: collection)
    {
      sb.append(item).append(", ");
    }
    final int len = sb.length();
    if( len >= 2)
      sb.delete(len-2, len);
    return "{" + sb.toString() + "}";
  }
  
  @Override
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }
  
}
