package com.datatorrent.alerts.notification.email;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.datatorrent.alerts.conf.EmailConfigRepo.EmailConfigCondition;
import com.google.common.collect.Sets;

/**
 * This class all the information to send an email
 * @author bright
 *
 */
public class EmailInfo {
  //The EMPTY is not safe, make sure don't change its property value
  protected static final EmailInfo EMPTY = new EmailInfo();
  
  protected String smtpServer;
  protected int smtpPort;
  protected String sender;
  protected char[] password;    //set password to null if support anonymous
  protected boolean enableTls;  //default is false
  
  protected Set<String> tos;
  protected Set<String> ccs;
  protected Set<String> bccs;
  
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
      newObj.tos = new HashSet<String>(tos);
    if(ccs != null)
      newObj.ccs = new HashSet<String>(ccs);
    if(bccs != null)
      newObj.bccs = new HashSet<String>(bccs);
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
      password = mergePolicy.merge(conf.context.entity.password == null ? null : String.valueOf(conf.context.entity.password), 
          password == null ? null : String.valueOf(password)).toCharArray();
      enableTls = Boolean.valueOf(mergePolicy.merge(String.valueOf(conf.context.entity.enableTls), String.valueOf(enableTls)));
    }
    if(conf != null && conf.recipients != null)
    {
      Set<String> newTos = Sets.newHashSet();
      Set<String> newCcs = Sets.newHashSet();
      Set<String> newBccs = Sets.newHashSet();
      for(MergableEntity<EmailRecipient> recipient : conf.recipients)
      {
        final MergePolicy mergePolicy = getMergePolicy(recipient);
        newTos.addAll(mergePolicy.merge(recipient.entity.tos, tos));
        newCcs.addAll(mergePolicy.merge(recipient.entity.ccs, ccs));
        newBccs.addAll(mergePolicy.merge(recipient.entity.bccs, bccs));
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
    
    MergePolicy mergePolicy = null;
    if(entity.entity instanceof MergePolicySupported)
      mergePolicy = ((MergePolicySupported)entity.entity).getMergePolicy();
    
    //use the default;
    return mergePolicy == null ? MergePolicy.configOverApp : mergePolicy;
  }
  
  @Override
  public int hashCode() {
    int hash = 7;
    hash = 41 * hash + (smtpServer != null ? smtpServer.hashCode() : 0);
    hash = 41 * hash + (sender != null ? sender.hashCode() : 0);
    hash = 41 * hash + (tos != null ? tos.hashCode() : 0);
    hash = 41 * hash + (subject != null ? subject.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final EmailInfo other = (EmailInfo) obj;
    if (this.smtpServer != other.smtpServer && (this.smtpServer == null || !this.smtpServer.equals(other.smtpServer))) {
      return false;
    }
    if (this.smtpPort != other.smtpPort ) {
      return false;
    }
    if (this.sender != other.sender && (this.sender == null || !this.sender.equals(other.sender))) {
      return false;
    }
    if (this.password != other.password && (this.password == null || !Arrays.equals(this.password, other.password))) {
      return false;
    }
    if (this.enableTls != other.enableTls) {
      return false;
    }
    if (this.tos != other.tos && (this.tos == null || !this.tos.equals(other.tos))) {
      return false;
    }
    if (this.ccs != other.ccs && (this.ccs == null || !this.ccs.equals(other.ccs))) {
      return false;
    }
    if (this.bccs != other.bccs && (this.bccs == null || !this.bccs.equals(other.bccs))) {
      return false;
    }
    if (this.subject != other.subject && (this.subject == null || !this.subject.equals(other.subject))) {
      return false;
    }
    if (this.body != other.body && (this.body == null || !this.body.equals(other.body))) {
      return false;
    }
    return true;
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

  public void setTos(final Collection<String> tos) {
    if(tos == null)
      return;
    if(tos instanceof Set)
      this.tos = (Set<String>)tos;
    this.tos = Sets.newHashSet();
    this.tos.addAll(tos);
  }

  public void setCcs(Collection<String> ccs) {
    if(ccs == null)
      return;
    if(ccs instanceof Set)
      this.ccs = (Set<String>)ccs;
    this.ccs = Sets.newHashSet();
    this.ccs.addAll(ccs);
  }

  public void setBccs(Collection<String> bccs) {
    if(bccs == null)
      return;
    if(bccs instanceof Set)
      this.bccs = (Set<String>)bccs;
    this.bccs = Sets.newHashSet();
    this.bccs.addAll(bccs);
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public void setBody(String body) {
    this.body = body;
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("smtpServer: ").append(smtpServer).append("; ");
    sb.append("smtpPort: ").append(smtpPort).append("; ");
    sb.append("sender: ").append(sender).append("; ");
    sb.append("password: ").append(password).append("; ");
    sb.append("enableTls: ").append(enableTls).append("; ");
    sb.append("tos: ").append(tos).append("; ");
    sb.append("ccs: ").append(ccs).append("; ");
    sb.append("bccs: ").append(bccs).append("; ");
    sb.append("subject: ").append(subject).append("; ");
    sb.append("body: ").append(body).append("; ");
    
    return sb.toString();
  }
}
