/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.lib.appdata.schemas.Message;

/**
 * 
 * This class decide which is the proper factory to deserialize
 *
 */
public class MessageDeserializerManagementFactory
{
  protected Class<? extends Message>[] schemas;
  
  protected MessageDeserializerFactory factoryForJson;
  protected SqlDeserializerFactory factoryForSql;
  
  protected Class<? extends Message> messageClass;
  protected Object context;
    
  public MessageDeserializerManagementFactory(Class<? extends Message>... schemas)
  {
    this.schemas = schemas;
  }
  

  /**
   * delegate the responsibility
   *
   */
  public Message deserialize(String s) throws IOException
  {
    if(s == null || s.isEmpty())
      return null;
    if(isJson(s))
    {
      return getFactoryForJson().deserialize(s);
    }

    return getFactoryForSql().deserialize(s);
  }
  
  protected boolean isJson(String s)
  {
    try
    {
      new JSONObject(s);
      return true;
    }
    catch(JSONException e)
    {
      return false;
    }
  }
  
  public void setContext(Class<? extends Message> messageClass, Object context)
  {
    this.messageClass = messageClass;
    this.context = context;
  }
  
  protected MessageDeserializerFactory getFactoryForJson()
  {
    if(factoryForJson == null)
    {
      factoryForJson = new MessageDeserializerFactory(schemas);
      factoryForJson.setContext(messageClass, context);
    }
    return factoryForJson;
  }
  
  protected SqlDeserializerFactory getFactoryForSql()
  {
    if(factoryForSql == null)
    {
      factoryForSql = new SqlDeserializerFactory(schemas);
      factoryForSql.setContext(messageClass, context);
    }
    return factoryForSql;
  }
}
