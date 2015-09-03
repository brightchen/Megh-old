package com.datatorrent.alerts;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.alerts.conf.EmailConfigRepo;
import com.datatorrent.alerts.conf.EmailConfigRepo.EmailConfigCondition;
import com.datatorrent.alerts.notification.email.EmailInfo;

/**
 * The resource to test send email.
 * @author bright
 *
 */
public class EmailConfTestResource {
  
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
        "<to>to1</to>" +
        "<to>to2</to>" +
        "<cc>cc1</cc>" +
        "<cc>cc2</cc>" +
        "<bcc>bcc1</bcc>" +
        "<bcc>bcc2</bcc>" +
        "</emailRecipient>"
    };
  
  public static final String[] contents =
    {
        "<emailContent>" +
        "<id>simplenotify</id>" +
        "<subject>subject1</subject>" +
        "<body>body1</body>" +
        "<mergePolicy>appOnly</mergePolicy>" +
        "</emailContent>",

        "<emailContent>" +
        "<id>2</id>" +
        "<subject>subject2</subject>" +
        "<body>body2</body>" +
        "</emailContent>"
    };
  
  public static final String[] criterias =
    {
        "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">simplenotify</emailContentRef>" +
        "</criteria>",

        "<criteria>" +
        "<emailContextRef mergePolicy=\"configOnly\">gmail</emailContextRef>" +
        "<emailRecipientRef mergePolicy=\"configOnly\">alladmin</emailRecipientRef>" +
        "<emailRecipientRef mergePolicy=\"appOverConf\">others</emailRecipientRef>" +
        "<emailContentRef mergePolicy=\"appOverConf\">simplenotify</emailContentRef>" +
        "</criteria>",

        
        "<criteria>" +
        "<app>app1</app>" +
        "<emailRecipientRef>others</emailRecipientRef>" +
        "</criteria>",

        "<criteria>" +
        "<level>1</level>" +
        "<emailRecipientRef>others</emailRecipientRef>" +
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
  
  //xml is composed by different sections;
  //public static final List<Map<Section, int[]>> xmls = new ArrayList<Map<Section, int[]>>();
  //one xml file map a map of input EmailInfo to output EmailInfo
  public static final Map<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,EmailInfo>>> testDatas 
    = new HashMap<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,EmailInfo>>>();
  static
  {
    Map<Section, int[]> xml0 = new EnumMap<Section, int[]>(Section.class);
    xml0.put(Section.context, new int[]{0});
    xml0.put(Section.recipient, new int[]{0});
    xml0.put(Section.content, new int[]{0});
    xml0.put(Section.criteria, new int[]{0});
    //xmls.add(xml0);
    
    Map<EmailInfo, Map<EmailConfigCondition,EmailInfo>> resultsPerXml = new HashMap<EmailInfo, Map<EmailConfigCondition,EmailInfo>>();
    EmailInfo expected = new EmailInfo();

    expected.setSmtpServer("smtp.gmail.com");
    expected.setSmtpPort(587);
    expected.setSender("datatorrent.alerts@gmail.com");
    expected.setPassword("password".toCharArray());
    expected.setEnableTls(true);
    expected.setTos(Arrays.asList("to1", "to2"));
    expected.setCcs(Arrays.asList("cc1", "cc2"));
    expected.setBccs(Arrays.asList("bcc1", "bcc2"));
    expected.setSubject("subject1");
    expected.setBody("body1");
    
    Map<EmailConfigCondition,EmailInfo> conditionResult = new HashMap<EmailConfigCondition,EmailInfo>();
    conditionResult.put(EmailConfigCondition.DEFAULT, expected);
    resultsPerXml.put(EmailInfo.EMPTY, conditionResult);
    
    testDatas.put(xml0, resultsPerXml);
    
    //Map<Section, int[]> xml1 = cloneMap( xml0, new EnumMap<Section, int[]>(Section.class) );
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
