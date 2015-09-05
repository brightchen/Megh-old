package com.datatorrent.alerts.notification.email;

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
  
//  public static EmailRecipient mergeAll( List<EmailRecipient> recipients )
//  {
//    Set<String> tos = Sets.newHashSet();
//    Set<String> ccs = Sets.newHashSet();
//    Set<String> bccs = Sets.newHashSet();
//    for(EmailRecipient recipient : recipients )
//    {
//      tos.addAll(recipient.tos);
//      ccs.addAll(recipient.ccs);
//      bccs.addAll(recipient.bccs);
//    }
//    return new EmailRecipient(tos, ccs, bccs);
//  }
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
    return String.format("tos: %s\nccs: %s\nbccs: %s\n", toString(tos), toString(ccs), toString(bccs));
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
    return "{" + sb.toString() + "}";
  }
  
  @Override
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }
  
}