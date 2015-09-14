package com.datatorrent.alerts.action.email;

public final class EmailContent implements MergePolicySupported {
  protected final String subject;
  protected final String body;
  protected final MergePolicy mergePolicy;
  
  public EmailContent(String subject, String body, String mergePolicy)
  {
    this(subject, body, MergePolicy.fromValue(mergePolicy));
  }
  public EmailContent(String subject, String body, MergePolicy mergePolicy) {
    this.subject = subject;
    this.body = body;
    this.mergePolicy = mergePolicy;
  }
  
  @Override
  public String toString()
  {
    return String.format("subject: %s; body: %s; mergePolicy %s", subject, body, mergePolicy);
  }
  
  @Override
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }
}
