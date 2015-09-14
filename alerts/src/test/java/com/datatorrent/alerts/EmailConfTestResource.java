package com.datatorrent.alerts;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datatorrent.alerts.action.email.EmailInfo;
import com.datatorrent.alerts.conf.EmailConfigRepo.EmailConfigCondition;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * The resource to test send email.
 * @author bright
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EmailConfTestResource {
  private final static EmailInfo EI_EMPTY = new EmailInfo();
  
  public static final String[] contexts =
    {
        "<emailContext>" +
        "<id>gmail</id>" +
        "<smtpServer>smtp.gmail.com</smtpServer>" +
        "<smtpPort>587</smtpPort>" +
        "<sender>datatorrent.alerts@gmail.com</sender>" +
        "<password>password</password>" +
        "<enableTls>true</enableTls>" +
        "<mergePolicy>configOnly</mergePolicy>" +
        "</emailContext>",
        
        "<emailContext>" +
        "<id>mailserver2</id>" +
        "<smtpServer>mailserver2</smtpServer>" +
        "<smtpPort>25</smtpPort>" +
        "<sender>sender2@gmail.com</sender>" +
        "<password>password2</password>" +
        "<enableTls>true</enableTls>" +
        "</emailContext>",
    };
  
  public static final String[] recipients =
    {
        "<emailRecipient>" +
        "<id>alladmin</id>" +
        "<to>to1</to>" +
        "<to>to2</to>" +
        "<cc>cc1</cc>" +
        "<cc>cc2</cc>" +
        "<bcc>bcc1</bcc>" +
        "<bcc>bcc2</bcc>" +
        "<mergePolicy>combine</mergePolicy>" +
        "</emailRecipient>",

        "<emailRecipient>" +
        "<id>others</id>" +
        "<to>others_to1</to>" +
        "<to>others_to2</to>" +
        "<cc>others_cc1</cc>" +
        "<cc>others_cc2</cc>" +
        "<bcc>others_bcc1</bcc>" +
        "<bcc>others_bcc2</bcc>" +
        "</emailRecipient>"
    };
  
  public static final String[] contents =
    {
        "<emailContent>" +
        "<id>defaultnotify</id>" +
        "<subject>subject1</subject>" +
        "<body>body1</body>" +
        "<mergePolicy>appOnly</mergePolicy>" +
        "</emailContent>",

        "<emailContent>" +
        "<id>app1</id>" +
        "<subject>subjectForApp1</subject>" +
        "<body>bodyForApp1</body>" +
        "</emailContent>",
        
        "<emailContent>" +
        "<id>level1</id>" +
        "<subject>subjectForLevel1</subject>" +
        "<body>bodyForLevel1</body>" +
        "</emailContent>"
    };
  
  public static final String[] criterias =
    {
        //overwrite default policy
        "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">defaultnotify</emailContentRef>" +
        "</criteria>",

        //use default policy
        "<criteria>" +
        "<emailContextRef>gmail</emailContextRef>" +
        "<emailRecipientRef>alladmin</emailRecipientRef>" +
        "<emailContentRef>defaultnotify</emailContentRef>" +
        "</criteria>",
            
        "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailRecipientRef mergePolicy=\"appOverConf\">others</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">defaultnotify</emailContentRef>" +
        "</criteria>",

        
        "<criteria>" +
        "<app>app1</app>" +
        "<emailContentRef>app1</emailContentRef>" +
        "</criteria>",

        "<criteria>" +
        "<level>1</level>" +
        "<emailContentRef>level1</emailContentRef>" +
        "</criteria>",

        "<criteria>" +
        "<app>app1</app>" +
        "<app>app2</app>" +
        "<level>1</level>" +
        "<level>2</level>" +
        "<emailRecipientRef>alladmin</emailRecipientRef>" +
        "</criteria>"
    };
  
  public static enum Section
  {
    context,
    recipient,
    content,
    criteria
  }
  
  public static Set<EmailInfo> getResultSet( EmailInfo ... results )
  {
    if(results == null || results.length == 0 )
      return null;
    Set<EmailInfo> resultSet = Sets.newHashSet();
    for( EmailInfo ei : results )
      if( ei.isComplete() )
        resultSet.add(ei);
    return resultSet;
  }
  
  //xml is composed by different sections;
  //public static final List<Map<Section, int[]>> xmls = new ArrayList<Map<Section, int[]>>();
  //one xml file map a map of input EmailInfo to output EmailInfo
  public static final Map<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>>> testDatas = Maps.newHashMap();
  static
  {
    /**
     * criteria
     * "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">defaultnotify</emailContentRef>" +
        "</criteria>",
     */
    Map<Section, int[]> xml0 = new EnumMap<Section, int[]>(Section.class);
    xml0.put(Section.context, new int[]{0});
    xml0.put(Section.recipient, new int[]{0});
    xml0.put(Section.content, new int[]{0});
    xml0.put(Section.criteria, new int[]{0});
    
    Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>> resultsXml0 = Maps.newHashMap();
    testDatas.put(xml0, resultsXml0);
    
    /**
     * test app don't have any data
     */
    {
      
      EmailInfo expected = new EmailInfo();
  
      expected.setSmtpServer("smtp.gmail.com");
      expected.setSmtpPort(587);
      expected.setSender("datatorrent.alerts@gmail.com");
      expected.setPassword("password".toCharArray());
      expected.setEnableTls(true);
      expected.setTos(Arrays.asList("to1", "to2"));
      expected.setCcs(Arrays.asList("cc1", "cc2"));
      expected.setBccs(Arrays.asList("bcc1", "bcc2"));
      expected.setSubject(null);
      expected.setBody(null);
      
      Map<EmailConfigCondition,Set<EmailInfo>> conditionResult = Maps.newHashMap();
      conditionResult.put(EmailConfigCondition.DEFAULT, getResultSet(expected));
      resultsXml0.put(EI_EMPTY, conditionResult);
    }
    
    /*
     * app have data, test overwrite policy
     */
    {
      EmailInfo input = new EmailInfo();
      input.setSmtpServer("appSmtpServer");
      input.setSmtpPort(25);
      input.setSender("appSender@gmail.com");
      input.setPassword("appPassword".toCharArray());
      input.setEnableTls(true);
      input.setTos(Arrays.asList("to1", "to2", "appto1", "appto2"));
      input.setBccs(Arrays.asList("bcc1" ));
      input.setSubject("appSubject");
      input.setBody("appBody");
      
      
      EmailInfo expected = new EmailInfo();
      
      expected.setSmtpServer("smtp.gmail.com");
      expected.setSmtpPort(587);
      expected.setSender("datatorrent.alerts@gmail.com");
      expected.setPassword("password".toCharArray());
      expected.setEnableTls(true);
      expected.setTos(Arrays.asList("to1", "to2"));
      expected.setCcs(Arrays.asList("cc1", "cc2"));
      expected.setBccs(Arrays.asList("bcc1", "bcc2"));
      expected.setSubject("appSubject");
      expected.setBody("appBody");
      
      Map<EmailConfigCondition,Set<EmailInfo>> conditionResult = Maps.newHashMap();
      conditionResult.put(EmailConfigCondition.DEFAULT, getResultSet(expected));
      resultsXml0.put(input, conditionResult);
    }

    /**
    "<emailContext>" +
    "<id>gmail</id>" +
    "<smtpServer>smtp.gmail.com</smtpServer>" +
    "<smtpPort>587</smtpPort>" +
    "<sender>datatorrent.alerts@gmail.com</sender>" +
    "<password>password</password>" +
    "<enableTls>true</enableTls>" +
    "<mergePolicy>configOnly</mergePolicy>" +
    "</emailContext>",
    
       "<emailRecipient>" +
        "<id>alladmin</id>" +
        "<to>to1</to>" +
        "<to>to2</to>" +
        "<cc>cc1</cc>" +
        "<cc>cc2</cc>" +
        "<bcc>bcc1</bcc>" +
        "<bcc>bcc2</bcc>" +
        "<mergePolicy>combine</mergePolicy>" +
        "</emailRecipient>",
        
        "<emailContent>" +
        "<id>defaultnotify</id>" +
        "<subject>subject1</subject>" +
        "<body>body1</body>" +
        "<mergePolicy>appOnly</mergePolicy>" +
        "</emailContent>",
    
    */
    Map<Section, int[]> xml1 = new EnumMap<Section, int[]>(Section.class);
    xml1.put(Section.context, new int[]{0});
    xml1.put(Section.recipient, new int[]{0});
    xml1.put(Section.content, new int[]{0});
    xml1.put(Section.criteria, new int[]{1});   //default overwrite policy
    
    Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>> resultsXml1 = Maps.newHashMap();
    testDatas.put(xml1, resultsXml1);
    {
      EmailInfo input = new EmailInfo();
      input.setSmtpServer("appSmtpServer");
      input.setSmtpPort(25);
      input.setSender("appSender@gmail.com");
      input.setPassword("appPassword".toCharArray());
      input.setEnableTls(true);
      input.setTos(Arrays.asList("to1", "to2", "appto1", "appto2"));
      input.setBccs(Arrays.asList("bcc1" ));
      input.setSubject("appSubject");
      input.setBody("appBody");
      
      
      EmailInfo expected = new EmailInfo();
      
      expected.setSmtpServer("smtp.gmail.com");
      expected.setSmtpPort(587);
      expected.setSender("datatorrent.alerts@gmail.com");
      expected.setPassword("password".toCharArray());
      expected.setEnableTls(true);
      
      expected.setTos(Arrays.asList("to1", "to2", "appto1", "appto2"));
      expected.setCcs(Arrays.asList("cc1", "cc2"));
      expected.setBccs(Arrays.asList("bcc1", "bcc2"));
      
      expected.setSubject("appSubject");
      expected.setBody("appBody");
      
      Map<EmailConfigCondition,Set<EmailInfo>> conditionResult = Maps.newHashMap();
      conditionResult.put(EmailConfigCondition.DEFAULT, getResultSet(expected));
      resultsXml1.put(input, conditionResult);
    }
    
    /** recipient has two groups
    "<criteria>" +
    "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
    "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
    "<emailRecipientRef mergePolicy=\"appOverConf\">others</emailRecipientRef>" +
    "<emailContentRef mergePolicy=\"appOverConf\">defaultnotify</emailContentRef>" +
    "</criteria>",
    */
    Map<Section, int[]> xml2 = new EnumMap<Section, int[]>(Section.class);
    xml2.put(Section.context, new int[]{0});
    xml2.put(Section.recipient, new int[]{0});
    xml2.put(Section.content, new int[]{0});
    xml2.put(Section.criteria, new int[]{1});   //default overwrite policy
    
    Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>> resultsXml2 = Maps.newHashMap();
    testDatas.put(xml2, resultsXml2);
    
    {
      EmailInfo input = new EmailInfo();
      input.setSmtpServer("appSmtpServer");
      input.setSmtpPort(25);
      input.setSender("appSender@gmail.com");
      input.setPassword("appPassword".toCharArray());
      input.setEnableTls(true);
      input.setTos(Arrays.asList("to1", "to2", "appto1", "appto2"));
      input.setBccs(Arrays.asList("appbcc1"));
      input.setSubject("appSubject");
      input.setBody("appBody");
      
      
      EmailInfo expected = new EmailInfo();
      
      expected.setSmtpServer("smtp.gmail.com");
      expected.setSmtpPort(587);
      expected.setSender("datatorrent.alerts@gmail.com");
      expected.setPassword("password".toCharArray());
      expected.setEnableTls(true);
      
      expected.setTos(Arrays.asList("to1", "to2", "appto1", "appto2"));
      expected.setCcs(Arrays.asList("cc1", "cc2"));
      expected.setBccs(Arrays.asList("bcc1", "bcc2","appbcc1"));
      
      expected.setSubject("appSubject");
      expected.setBody("appBody");
      
      Map<EmailConfigCondition,Set<EmailInfo>> conditionResult = Maps.newHashMap();
      conditionResult.put(EmailConfigCondition.DEFAULT, getResultSet(expected));
      resultsXml2.put(input, conditionResult);
    }
    
    /**
        //overwrite default policy
        "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">defaultnotify</emailContentRef>" +
        "</criteria>",

        //use default policy
        "<criteria>" +
        "<emailContextRef>gmail</emailContextRef>" +
        "<emailRecipientRef>alladmin</emailRecipientRef>" +
        "<emailContentRef>defaultnotify</emailContentRef>" +
        "</criteria>",
            
        "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailRecipientRef mergePolicy=\"appOverConf\">others</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">defaultnotify</emailContentRef>" +
        "</criteria>",

        
        "<criteria>" +
        "<app>app1</app>" +
        "<emailContentRef>app1</emailContentRef>" +
        "</criteria>",

        "<criteria>" +
        "<level>1</level>" +
        "<emailContentRef>level1</emailContentRef>" +
        "</criteria>",

        "<criteria>" +
        "<app>app1</app>" +
        "<app>app2</app>" +
        "<level>1</level>" +
        "<level>2</level>" +
        "<emailRecipientRef>alladmin</emailRecipientRef>" +
        "</criteria>"
     */
    Map<Section, int[]> xmlAll = new EnumMap<Section, int[]>(Section.class);
    xmlAll.put(Section.context, new int[]{0,1});
    xmlAll.put(Section.recipient, new int[]{0,1});
    xmlAll.put(Section.content, new int[]{0,1,2});
    xmlAll.put(Section.criteria, new int[]{0,1,2,3,4,5});   //default overwrite policy
    
    Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>> resultsXmlAll = Maps.newHashMap();
    testDatas.put(xmlAll, resultsXmlAll);
    
    {
      EmailInfo input = new EmailInfo();
      input.setSmtpServer("appSmtpServer");
      input.setSmtpPort(25);
      input.setSender("appSender@gmail.com");
      input.setPassword("appPassword".toCharArray());
      input.setEnableTls(true);
      input.setTos(Arrays.asList("to2", "appto1", "appto2"));
      input.setBccs(Arrays.asList("appbcc1"));
      input.setSubject("appSubject");
      input.setBody("appBody");
      
      //check (app1,2) only, as criteria (app1,2)'s merge policy is default ConfigOverApp, 
      //so the app information will be merged and information is complete and will not check the (app1,null) and (null,2) etc
      final EmailConfigCondition condition = new EmailConfigCondition("app1", 2);
      
      EmailInfo expected = new EmailInfo();
      
      expected.setSmtpServer("appSmtpServer");
      expected.setSmtpPort(25);
      expected.setSender("appSender@gmail.com");
      expected.setPassword("appPassword".toCharArray());
      expected.setEnableTls(true);
      
      expected.setTos(Arrays.asList("to1", "to2", "appto1", "appto2"));
      expected.setCcs(Arrays.asList("cc1", "cc2"));
      expected.setBccs(Arrays.asList("bcc1", "bcc2","appbcc1"));
      
      expected.setSubject("appSubject");
      expected.setBody("appBody");
      
      Map<EmailConfigCondition,Set<EmailInfo>> conditionResult = Maps.newHashMap();
      conditionResult.put(condition, getResultSet(expected));
      resultsXmlAll.put(input, conditionResult);
    }
    
    {
      //(app1,1) =>(app1) || (1)=> ()
      final EmailConfigCondition condition = new EmailConfigCondition("app1", 1);
      
      EmailInfo expected1 = new EmailInfo();
      
      expected1.setSmtpServer("smtp.gmail.com");
      expected1.setSmtpPort(587);
      expected1.setSender("datatorrent.alerts@gmail.com");
      expected1.setPassword("password".toCharArray());
      expected1.setEnableTls(true);
      
      expected1.setTos(Arrays.asList("to1", "to2", "others_to1", "others_to2"));
      expected1.setCcs(Arrays.asList("cc1", "cc2", "others_cc1", "others_cc2"));
      expected1.setBccs(Arrays.asList("bcc1", "bcc2", "others_bcc1", "others_bcc2"));
      
      expected1.setSubject("subjectForApp1");
      expected1.setBody("bodyForApp1");
      
      EmailInfo expected2 = expected1.clone();
      expected2.setSubject("subjectForLevel1");
      expected2.setBody("bodyForLevel1");
      
      Map<EmailConfigCondition,Set<EmailInfo>> conditionResult = Maps.newHashMap();
      conditionResult.put(condition, getResultSet(expected1, expected2));
      resultsXmlAll.put(EI_EMPTY, conditionResult);
    }
  }
  
  public static String getXml(Map<Section, int[]> xmlSections)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n");
    sb.append("<conf>\n");
    for(Map.Entry<Section, int[]> entry : xmlSections.entrySet())
    {
      for(int index : entry.getValue())
      {
        sb.append(getSectionString(entry.getKey(), index));
      }
    }
    sb.append("</conf>\n");
    return sb.toString();
  }
  
  public static String getSectionString(Section section, int index)
  {
    switch(section)
    {
    case context:
      return contexts[index];
    case recipient:
      return recipients[index];
    case content:
      return contents[index];
    case criteria:
      return criterias[index];
    }
    throw new IllegalArgumentException("Unsupported section: " + section);
  }
  
  public static Map<Section, int[]> cloneMap( Map<Section, int[]> src, Map<Section, int[]> dest )
  {
    for(Map.Entry<Section, int[]> entry : src.entrySet())
    {
      dest.put(entry.getKey(), entry.getValue());
    }
    return dest;
  }
 
}
