package com.datatorrent.alerts.notification.email;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This class all the information to send an email
 * @author bright
 *
 */
public class EmailInfo {
  public static final EmailInfo EMPTY = new EmailInfo();
  
  protected String smtpServer;
  protected int smtpPort;
  protected String sender;
  protected char[] password;    //set password to null if support anonymous
  protected boolean enableTls;  //default is false
  
  protected Collection<String> tos;
  protected Collection<String> ccs;
  protected Collection<String> bccs;
  
  protected String subject;
  protected String body;
  
  public void verifyEnoughInfoForSendEmail() throws LackInfoException
  {
    if(smtpServer == null || smtpServer.isEmpty())
      throw new LackInfoException("smtp server is empty.");
    if(sender == null || sender.isEmpty())
      throw new LackInfoException("sender is empty.");
    if( !hasValidEntry( tos ) )
      throw new LackInfoException("to is empty.");
    if(subject == null || subject.isEmpty())
      throw new LackInfoException("subject is empty.");
  }
  
  public boolean isComplete()
  {
    return ( smtpServer != null && !smtpServer.isEmpty() && smtpPort != 0 && sender != null && !sender.isEmpty() 
        && hasValidEntry( tos ) && ( subject != null && !subject.isEmpty() ) );
  }
  
  public static boolean hasValidEntry( Collection<String> collection )
  {
    if( collection == null || collection.isEmpty() )
      return false;
    for( String entry : collection )
    {
      if( entry != null && !entry.isEmpty() )
        return true;
    }
    return false;
  }
  
  @Override
  public EmailInfo clone()
  {
    EmailInfo newObj = new EmailInfo();
    newObj.smtpServer = smtpServer;
    newObj.smtpPort = smtpPort;
    newObj.sender = sender;
    if(password != null)
      newObj.password = Arrays.copyOf(password, password.length);
    newObj.enableTls = enableTls;
    if(tos != null)
      newObj.tos = new ArrayList<String>(tos);
    if(ccs != null)
      newObj.ccs = new ArrayList<String>(ccs);
    if(bccs != null)
      newObj.bccs = new ArrayList<String>(bccs);
    newObj.subject = subject;
    newObj.body = body;
    
    return newObj;
  }
  
  /**
   * merge the configuration according the merge policy
   * @param conf
   * @return
   */
  public EmailInfo mergeWith(EmailConf conf)
  {
    if(conf == null)
      return this;
    
    if(conf.context != null)
    {
      final MergePolicy mergePolicy = getMergePolicy(conf.context);
      smtpServer = mergePolicy.merge(conf.context.entity.smtpServer, smtpServer);
      smtpPort = Integer.parseInt( mergePolicy.merge(conf.context.entity.smtpPort+"", smtpPort+"") );
      sender = mergePolicy.merge(conf.context.entity.sender, sender);
      enableTls = Boolean.valueOf(mergePolicy.merge(String.valueOf(conf.context.entity.enableTls), String.valueOf(enableTls)));
    }
    if((tos==null||tos.isEmpty()) && conf != null && conf.recipients != null)
    {
      Set<String> newTos = new HashSet<String>();
      Set<String> newCcs = new HashSet<String>();
      Set<String> newBccs = new HashSet<String>();
      for(MergableEntity<EmailRecipient> recipient : conf.recipients)
      {
        newTos.addAll(recipient.mergePolicy.merge(recipient.entity.tos, tos));
        newCcs.addAll(recipient.mergePolicy.merge(recipient.entity.ccs, ccs));
        newBccs.addAll(recipient.mergePolicy.merge(recipient.entity.bccs, bccs));
      }
      tos = newTos;
      ccs = newCcs;
      bccs = newBccs;
    }
    if(conf.content != null)
    {
      final MergePolicy mergePolicy = getMergePolicy(conf.content);
      subject = mergePolicy.merge(conf.content.entity.subject, subject);
      body = mergePolicy.merge(conf.content.entity.body, body);
    }
    return this;
  }
  
  protected MergePolicy getMergePolicy(MergableEntity<?> entity)
  {
    if(entity.mergePolicy != null)
      return entity.mergePolicy;
    if(entity.entity instanceof MergePolicySupported)
      return ((MergePolicySupported)entity.entity).getMergePolicy();
    
    //use the default;
    return MergePolicy.configOverApp;
  }
  
  public void setSmtpServer(String smtpServer) {
    this.smtpServer = smtpServer;
  }

  public void setSmtpPort(int smtpPort) {
    this.smtpPort = smtpPort;
  }

  public void setSender(String sender) {
    this.sender = sender;
  }

  public void setPassword(char[] password) {
    this.password = password;
  }

  public void setEnableTls(boolean enableTls) {
    this.enableTls = enableTls;
  }

  public void setTos(Collection<String> tos) {
    this.tos = tos;
  }

  public void setCcs(Collection<String> ccs) {
    this.ccs = ccs;
  }

  public void setBccs(Collection<String> bccs) {
    this.bccs = bccs;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public void setBody(String body) {
    this.body = body;
  }
  
  
}
