package com.datatorrent.alerts.notification.email;

public final class EmailContext{
  protected final String smtpServer;
  protected final int smtpPort;
  protected final String sender;
  protected final char[] password;    //set password to null if support anonymous
  protected final boolean enableTls;
  protected final MergePolicy mergePolicy;
  
  public EmailContext( String smtpServer, int smtpPort, String sender, char[] password, boolean enableTls, String mergePolicy )
  {
    this(smtpServer, smtpPort, sender, password, enableTls, MergePolicy.fromValue(mergePolicy));
  }
  
  public EmailContext( String smtpServer, int smtpPort, String sender, char[] password, boolean enableTls, MergePolicy mergePolicy )
  {
    this.smtpServer = smtpServer;
    this.smtpPort = smtpPort;
    this.sender = sender;
    this.password = password;
    this.enableTls = enableTls;
    this.mergePolicy = mergePolicy;
  }
  
  @Override
  public String toString()
  {
    return String.format("smtpServer: %s; smtpPort: %d; sender: %s; has Password: %b; enableTls: %b", 
        smtpServer, smtpPort, sender, Boolean.valueOf(password!=null), Boolean.valueOf(enableTls));
  }
}
