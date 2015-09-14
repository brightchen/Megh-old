package com.datatorrent.alerts.action.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.alerts.conf.AlertsProperties;
import com.google.common.collect.Sets;

/**
 * This class get while list from file.
 * each line of the line contains a command(case-sensitive)
 * @author bright
 *
 */
public class DefaultWhiteListProvider implements WhiteListProvider{
  
  private static final Logger logger = LoggerFactory.getLogger(DefaultWhiteListProvider.class);
  public static final String PROP_ALERTS_COMMAND_WHITELIST_CONF_FILE = "alerts.command.whitelist.file";
  public static final String DEFAULT_ALERTS_COMMAND_WHITELIST_CONF_FILE = "WhiteListConf.conf";
  
  private final Configuration conf = new Configuration();

  private Set<String> whiteList = Sets.newHashSet();

  public DefaultWhiteListProvider()
  {
    loadConfig();
  }
  
  @Override
  public Set<String> getWhiteList() {
    return whiteList;
  }

  @Override
  public boolean refresh()
  {
    Set<String> preList = Sets.newHashSet(whiteList);
    loadConfig();
    boolean changed = (preList.size() != whiteList.size());
    if(changed)
      return true;
    preList.removeAll(whiteList);
    return !preList.isEmpty();
  }
  
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
  

  protected void loadConfig(InputStream inputStream)
  {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      
      while(true)
      {
        String line = reader.readLine();
        if(line == null)
          break;
        line = line.trim();
        if(line.isEmpty() || line.startsWith("#"))
          continue;
        whiteList.add(line);
      }
      
    } catch (Exception e) {
      logger.error("Get or parse configure file exception.", e);
    }
  }
  
  protected InputStream getConfigInputStream()
  {
    //read from hdfs 
    String fileName = getConfigFile();
    File file = new File(fileName);
    logger.info("absolute file path: {}", file.getAbsolutePath() );
    logger.info("Command white list configure file path: {}", fileName);
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
  

  protected String getConfigFile()
  {
    //get from system properties first
    String filePath = System.getProperty(PROP_ALERTS_COMMAND_WHITELIST_CONF_FILE);
    if( filePath == null || filePath.isEmpty() )
    {
      //get from alerts config file
      filePath = AlertsProperties.instance().getProperty(PROP_ALERTS_COMMAND_WHITELIST_CONF_FILE);
    }
    if( filePath == null || filePath.isEmpty() )
    {
      //load default conf file;
      logger.warn("Can not get command white list configure file informaiton from system property or alerts configuration. try to load config file from class path.");
      URL fileUrl = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_ALERTS_COMMAND_WHITELIST_CONF_FILE);
      if(fileUrl != null)
        filePath = fileUrl.getPath();
    }
    if( filePath == null || filePath.isEmpty() )
    {
      logger.warn("Can not get command white list configure file informaiton from system property, alerts configuration or class path. use default.");
      filePath = DEFAULT_ALERTS_COMMAND_WHITELIST_CONF_FILE;
    }
    logger.info("Command white list Config file path: {}", filePath);
    return filePath;
  }
  
}
