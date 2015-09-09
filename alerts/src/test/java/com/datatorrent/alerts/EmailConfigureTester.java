package com.datatorrent.alerts;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  
  private static final Logger logger = LoggerFactory.getLogger(TestEmailConfigRepo.class);
  
  public void testLoadConfigure()
  {
    DefaultEmailConfigRepo.instance();
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void test()
  {
    Map<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>>> testDatas = EmailConfTestResource.testDatas;
    int fileCount = 0;
    int caseCount = 0;
    for( Map.Entry<Map<Section, int[]>, Map<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>>> entry : testDatas.entrySet() )
    {
      String xml = EmailConfTestResource.getXml(entry.getKey());
      TestEmailConfigRepo repo = new TestEmailConfigRepo(xml);
      repo.loadConfig();

      for(Map.Entry<EmailInfo, Map<EmailConfigCondition,Set<EmailInfo>>> resultEntry : entry.getValue().entrySet())
      {
        for(Map.Entry<EmailConfigCondition,Set<EmailInfo>> conditionMap : resultEntry.getValue().entrySet())
        {
          EmailConfigCondition condition = conditionMap.getKey();
          EmailInfo input = resultEntry.getKey().clone();
          List<EmailInfo> actualResults = repo.fillEmailInfo(condition.getApp()==null?"":condition.getApp(), 
              condition.getLevel()==null?0:condition.getLevel(), input);
          if(actualResults==null)
            Assert.assertTrue(conditionMap.getValue()==null || conditionMap.getValue().isEmpty());
          else
          {
//            Assert.assertTrue(actualResults.size()==conditionMap.getValue().size());
            Assert.assertArrayEquals(conditionMap.getValue().toArray(), new HashSet(actualResults).toArray() );
          }
          caseCount++;
        }
        
      }
      ++fileCount;
    }
    logger.info("Tested {} files, {} cases.", fileCount, caseCount);
  }
}
