/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;

import com.datatorrent.lib.appdata.schemas.Message;

/**
 * Node: TODO: This class right now is in Megh, it could move to Malhar if also support sql for snapshot
 * and in fact, there don't have too much difference between snapshot and dimensional except time bucket
 * 
 *
 */
public class SqlDeserializerFactory extends MessageDeserializerFactory
{
  protected static final String type = "dataQuery";
  
  protected DataQuerySqlDeserializer  dataQueryDeserializer;
  
  protected SqlDeserializerFactory(Class<? extends Message>... schemas)
  {
    super(schemas);
  }
  
  /**
   * The input should be only pure sql. it will include any other information such as FIELD_TYPE etc.
   *
   */
  public Message deserialize(String sql) throws IOException
  {
    Message data = dataQueryDeserializer.deserialize(sql, null, null);

    data.setType(type);
    return data;
  }
}
