package com.datatorrent.alerts;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.alerts.EmailConfTestResource.Section;
import com.datatorrent.alerts.conf.DefaultEmailConfigRepo;
import com.datatorrent.alerts.conf.EmailConfigRepo.EmailConfigCondition;
import com.datatorrent.alerts.notification.email.EmailInfo;

public class EmailConfigureTester {
  public static class TestEmailConfigRepo extends DefaultEmailConfigRepo
  {
    protected String xml;
    public TestEmailConfigRepo(String xml)
    {
      this.xml = xml;
    }
    @Override
    protected InputStream getConfigInputStream()
    {
      return new ByteArrayInputStream(xml.getBytes());
    }
    @Override
    public void loadConfig()
    {
      super.loadConfig();
    }
  }
  
  @Test
  public void testLoadConfigure()
  {
    DefaultEmailConfigRepo.instance();
  }
  
  @Test
  public void test()
  {
    Map<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,EmailInfo>>> testDatas = EmailConfTestResource.testDatas;
    for( Map.Entry<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,EmailInfo>>> entry : testDatas.entrySet() )
    {
      String xml = EmailConfTestResource.getXml(entry.getKey());
      TestEmailConfigRepo repo = new TestEmailConfigRepo(xml);
      repo.loadConfig();
      for(Map.Entry<EmailInfo, Map<EmailConfigCondition,EmailInfo>> resultEntry : entry.getValue().entrySet())
      {
        for(Map.Entry<EmailConfigCondition,EmailInfo> conditionMap : resultEntry.getValue().entrySet())
        {
          EmailConfigCondition condition = conditionMap.getKey();
          EmailInfo input = resultEntry.getKey().clone();
          List<EmailInfo> actualResults = repo.fillEmailInfo(condition.getApp()==null?"":condition.getApp(), 
              condition.getLevel()==null?0:condition.getLevel(), input);
          if(conditionMap.getValue().isComplete())
          {
            Assert.assertTrue(actualResults.size()==1);
            Assert.assertTrue(actualResults.get(0).equals(conditionMap.getValue()));
          }
          else
            Assert.assertTrue(actualResults==null);
        }
        
      }
    }
    DefaultEmailConfigRepo repo = DefaultEmailConfigRepo.instance();
  }
}
