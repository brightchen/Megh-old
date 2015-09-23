package com.datatorrent.alerts.action.email;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.alerts.action.email.xmlbind.Conf;
import com.datatorrent.alerts.conf.AlertsProperties;
import com.google.common.collect.Lists;

/**
 * 
 * @author bright
 * DefaultEmailConfigRepo load configuration from HDFS file.
 * 
 */
public class DefaultEmailConfigRepo extends EmailConfigRepo {

  public static class MutableEmailConf {
    protected MergableEntity<EmailContext> context;
    protected Collection<MergableEntity<EmailRecipient>> recipients;
    protected MergableEntity<EmailContent> content;
    
    public MutableEmailConf(){}

    public MutableEmailConf(MergableEntity<EmailContext> context, Collection<MergableEntity<EmailRecipient>> recipients, 
        MergableEntity<EmailContent> content) 
    {
      setValue(context, recipients, content);
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

  
  private static final Logger logger = LoggerFactory.getLogger(DefaultEmailConfigRepo.class);
      
  public static final String PROP_ALERTS_EMAIL_CONF_FILE = "alerts.email.conf.file";
  public static final String DEFAULT_EMAIL_CONF_FILE = "EmailNotificationConf.xml";
  
  private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private static DefaultEmailConfigRepo instance = null;
  
  private final Configuration conf = new Configuration();
  private Map<EmailConfigCondition, MutableEmailConf> mutableEmailConfMap = new HashMap<EmailConfigCondition, MutableEmailConf>();
  
  public static DefaultEmailConfigRepo instance()
  {
    if( instance == null )
    {
      synchronized(DefaultEmailConfigRepo.class)
      {
        if( instance == null )
        {
          instance = new DefaultEmailConfigRepo();
          instance.loadConfig();
        }
      }
    }
    return instance;
  }
  
  /**
   * when refresh, create a new instance instead of use old instance 
   * to avoid block the system and avoid lock.
   * TODO: should break a previous fresh if a new refresh was asked before previous still doing.
   */
  public void refresh()
  {
    DefaultEmailConfigRepo newInstance = new DefaultEmailConfigRepo();
    newInstance.loadConfig();
    
    rwLock.writeLock().lock();
    try
    {
      newInstance.cloneTo(this);
    }
    finally
    {
      rwLock.writeLock().unlock();
    }
  }
  
  protected void cloneTo(DefaultEmailConfigRepo other)
  {
    super.cloneTo(other);
  }
  
  protected String getConfigFile()
  {
    //get from system properties first
    String filePath = System.getProperty(PROP_ALERTS_EMAIL_CONF_FILE);
    if( filePath == null || filePath.isEmpty() )
    {
      //get from alerts config file
      filePath = AlertsProperties.instance().getProperty(PROP_ALERTS_EMAIL_CONF_FILE);
    }
    if( filePath == null || filePath.isEmpty() )
    {
      //load default conf file;
      logger.warn("Can not get email configure file informaiton from system property or alerts configuration. try to load config file from class path.");
      URL fileUrl = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_EMAIL_CONF_FILE);
      if(fileUrl != null)
        filePath = fileUrl.getPath();
    }
    if( filePath == null || filePath.isEmpty() )
    {
      logger.warn("Can not get email configure file informaiton from system property, alerts configuration or class path. use default.");
      filePath = DEFAULT_EMAIL_CONF_FILE;
    }
    logger.info("Email Notification Config file path: {}", filePath);
    return filePath;
  }
  
  @Override
  protected void loadConfig() {
    InputStream inputStream = null;
    try
    {
      inputStream = getConfigInputStream();
      loadConfig(inputStream);
    }
    catch(Exception e)
    {
      logger.error("Get or parse configure file exception.", e);
    }
    finally
    {
      IOUtils.closeQuietly(inputStream);
    }
  }
  
  protected InputStream getConfigInputStream()
  {
    //read from hdfs 
    String fileName = getConfigFile();
    File file = new File(fileName);
    logger.info("absolute file path: {}", file.getAbsolutePath() );
    logger.info("Email configure file path: {}", fileName);
    Path filePath = new Path(fileName);
    FSDataInputStream inputStream = null;
    try
    {
      FileSystem fs = FileSystem.get(conf);
      return fs.open(filePath);
    }
    catch(Exception e)
    {
      logger.error("Get or parse configure file exception.", e);
      return null;
    }
  }
  
  protected void loadConfig(InputStream inputStream)
  {
    try {
      //set support classes
      JAXBContext context = JAXBContext.newInstance(Conf.class, Conf.EmailContext.class, Conf.EmailContent.class, Conf.EmailRecipient.class, Conf.Criteria.class);
      //JAXBContext context = JAXBContext.newInstance(EmailContextMutable.class, EmailContentMutable.class, EmailRecipientMutable.class);
      
      Unmarshaller unmarshaller = context.createUnmarshaller();
      Conf conf = (Conf)unmarshaller.unmarshal(inputStream);
      logger.debug(conf.toString());
      
      if(conf != null)
        loadConfig(conf);
      
    } catch (Exception e) {
      logger.error("Get or parse configure file exception.", e);
    }
  }

  protected void loadConfig(Conf conf)
  {
    List<Conf.Criteria> criterias = conf.getCriteria();
    if( criterias == null || criterias.isEmpty() )
      return;
    
    Map<String, EmailContext> contextMap = new HashMap<String, EmailContext>();
    {
      List<Conf.EmailContext> contexts = conf.getEmailContext();
      if(contexts != null)
      {
        for(Conf.EmailContext context : contexts )
        {
          contextMap.put(context.getId(), getEmailContext(context) );
        }
      }
    }
    
    Map<String, EmailContent> contentMap = new HashMap<String, EmailContent>();
    {
      List<Conf.EmailContent> contents = conf.getEmailContent();
      if(contents != null)
      {
        for(Conf.EmailContent content : contents )
        {
          contentMap.put(content.getId(), getEmailContent(content) );
        }
      }
    }
    
    Map<String, EmailRecipient> recipientMap = new HashMap<String, EmailRecipient>();
    {
      List<Conf.EmailRecipient> recipients = conf.getEmailRecipient();
      if(recipients != null)
      {
        for(Conf.EmailRecipient recipient : recipients )
        {
          recipientMap.put(recipient.getId(), getEmailRecipient(recipient) );
        }
      }
    }
    
    for(Conf.Criteria criteria : criterias)
    {
      List<String> apps = criteria.getApp();
      List<Integer> levels = criteria.getLevel();
      
      List<EmailConfigCondition> conditions = getConditions( apps, levels );

      for( EmailConfigCondition condition : conditions )
      {
        MergableEntity<EmailContext> context = null;
        if( contextMap != null && !contextMap.isEmpty() && criteria.getEmailContextRef() != null )
        {
          EmailContext contextValue = contextMap.get(criteria.getEmailContextRef().getValue());
          if(contextValue == null)
            logger.warn("The email context ref {} is invalid.", criteria.getEmailContextRef());
          else
            context = new MergableEntity<EmailContext>(contextValue, criteria.getEmailContextRef().getMergePolicy() );
        }
        
        //it is possible there are multiple recipient for each criteria
        List<MergableEntity<EmailRecipient>> recipients = Lists.newArrayList();
        if( recipientMap != null && !recipientMap.isEmpty() && criteria.getEmailRecipientRef() !=null )
        {
          for(Conf.Criteria.EmailRecipientRef recipientRef : criteria.getEmailRecipientRef())
          {
            String recipientId = recipientRef.getValue();
            recipients.add(new MergableEntity<EmailRecipient>(recipientMap.get(recipientId), recipientRef.getMergePolicy()));
          }
        }
        MergableEntity<EmailContent> content = null;
        if( contentMap != null && !contentMap.isEmpty() && criteria.getEmailContentRef() !=null )
        {
          final EmailContent contentValue = contentMap.get(criteria.getEmailContentRef().getValue());
          if(contentValue == null)
            logger.warn("The email content ref {} is invalid.", criteria.getEmailContentRef());
          else
            content = new MergableEntity<EmailContent>(contentValue, criteria.getEmailContentRef().getMergePolicy());
        }
        
        mergeConfig(mutableEmailConfMap, condition, context, recipients, content);
      }
    }
    
    immutableConfig();
    dumpEmailConf();
    
    logger.info("Load configuration done.");
  }
  
  protected void immutableConfig()
  {
    Map<EmailConfigCondition, EmailConf> emailConfMap = getEmailConfMap();
    emailConfMap.clear();
    for( Map.Entry<EmailConfigCondition, MutableEmailConf> entry : mutableEmailConfMap.entrySet() )
    {
      emailConfMap.put(entry.getKey(), createEmailConf(entry.getValue()));
    }
  }
  
  protected static EmailConf createEmailConf(MutableEmailConf mutableEmailConf)
  {
    return new EmailConf(mutableEmailConf.context, mutableEmailConf.recipients, mutableEmailConf.content);
  }
  
  public List<EmailInfo> fillEmailInfo(String appName, int level, EmailInfo inputEmailInfo) 
  {
    rwLock.readLock().lock();
    try {
      return super.fillEmailInfo(appName, level, inputEmailInfo);
    } finally {
      rwLock.readLock().unlock();
    }
  }
  
 
  protected static void mergeConfig(Map<EmailConfigCondition, MutableEmailConf> mutableEmailConfMap, EmailConfigCondition condition,
      MergableEntity<EmailContext> context, Collection<MergableEntity<EmailRecipient>> recipients, MergableEntity<EmailContent> content )
  {

    MutableEmailConf conf = mutableEmailConfMap.get(condition);
    if(conf == null)
    {
      conf = new MutableEmailConf();
      mutableEmailConfMap.put(condition, conf);
    }
    // merge in fact is override
    conf.setValue(context, recipients, content);
  }
  
  protected static List<EmailConfigCondition> getConditions( List<String> apps, List<Integer> levels )
  {
    if( (apps == null || apps.isEmpty()) && (levels == null || levels.isEmpty()) )
      return Lists.newArrayList(EmailConfigCondition.DEFAULT);
    
    List<EmailConfigCondition> conditions = new ArrayList<EmailConfigCondition>();
    if(apps == null || apps.isEmpty())
    {
      for( Integer level : levels )
        conditions.add( new EmailConfigCondition(level) );
      return conditions;
    }
    if(levels == null || levels.isEmpty())
    {
      for( String app : apps )
        conditions.add( new EmailConfigCondition(app) );
      return conditions;
    }
    for(String app : apps)
    {
      for(Integer level : levels)
        conditions.add( new EmailConfigCondition(app, level) );
    }
    return conditions;
  }
  
  protected static EmailContext getEmailContext(Conf.EmailContext confContext)
  {
    return new EmailContext(confContext.getSmtpServer(), confContext.getSmtpPort(), confContext.getSender(), 
        confContext.getPassword()==null ? null : confContext.getPassword().toCharArray(), confContext.isEnableTls(), confContext.getMergePolicy() );
  }
  
  protected static EmailContent getEmailContent(Conf.EmailContent confContent)
  {
    return new EmailContent(confContent.getSubject(), confContent.getBody(), confContent.getMergePolicy());
  }
  
  protected static EmailRecipient getEmailRecipient(Conf.EmailRecipient confRecipient)
  {
    return new EmailRecipient(confRecipient.getTo(), confRecipient.getCc(), confRecipient.getBcc(), confRecipient.getMergePolicy());
  }
}