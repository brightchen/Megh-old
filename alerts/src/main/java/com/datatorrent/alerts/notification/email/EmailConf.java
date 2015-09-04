package com.datatorrent.alerts.notification.email;

import java.util.Collection;

import com.google.common.collect.ImmutableList;

public final class EmailConf {
  protected final MergableEntity<EmailContext> context;
  protected final ImmutableList<MergableEntity<EmailRecipient>> recipients;
  protected final MergableEntity<EmailContent> content;
  

  public EmailConf(MergableEntity<EmailContext> context, Collection<MergableEntity<EmailRecipient>> recipients, 
      MergableEntity<EmailContent> content) 
  {
    
    this.context = context;
    this.recipients = ImmutableList.copyOf(recipients);
    this.content = content;
  }
  
//  public void setValue(MergableEntity<EmailContext> context, Collection<MergableEntity<EmailRecipient>> recipients, 
//      MergableEntity<EmailContent> content) {
//    this.context = context;
//    this.recipients = recipients;
//    this.content = content;
//
//  }
  
  @Override
  public String toString()
  {
    return String.format("context:{%s}\n recipients:{%s}\n content: {%s}\n", context, recipients, content);
  }
}
