package com.datatorrent.alerts.notification.email;

import java.util.Collection;

public final class EmailConf {
  protected MergableEntity<EmailContext> context;
  protected Collection<MergableEntity<EmailRecipient>> recipients;
  protected MergableEntity<EmailContent> content;
  
  public EmailConf() {
  }
  public EmailConf(MergableEntity<EmailContext> context, Collection<MergableEntity<EmailRecipient>> recipients, 
      MergableEntity<EmailContent> message) 
  {
    setValue(context, recipients, message);
  }
  
  public void setValue(MergableEntity<EmailContext> context, Collection<MergableEntity<EmailRecipient>> recipients, 
      MergableEntity<EmailContent> content) {
    this.context = context;
    this.recipients = recipients;
    this.content = content;
  }
  
  @Override
  public String toString()
  {
    return String.format("context:{%s}\n recipients:{%s}\n content: {%s}\n", context, recipients, content);
  }
}
