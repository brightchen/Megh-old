/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Collections;
import java.util.Map;

import com.datatorrent.lib.appdata.gpo.Serde;
import com.datatorrent.lib.appdata.gpo.SerdeListPrimitive;
import com.datatorrent.lib.appdata.gpo.SerdeMapPrimitive;
import com.datatorrent.lib.appdata.gpo.SerdeObjectPayloadFix;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

/**
 * the super class FieldsDescriptor don't allow to modify/set serdes.
 * Create this class from walk around. 
 * NOTE: this class can't removed after FieldsDescriptor allow to modify/set serdes
 *
 */
public class CompositeAggregatorFieldsDescriptor extends FieldsDescriptor
{
  private static final long serialVersionUID = -6048545385207591467L;
  
  //try to share one array to avoid memory operate.
  protected static final int DEFAULT_COMPOSITE_SERDES_SIZE = 100;
  protected static Serde[] DEFAULT_COMPOSITE_SERDES;
  
  static
  {
    createDefaultCompositeSerdes(DEFAULT_COMPOSITE_SERDES_SIZE);
  }
  
//For kryo
  protected CompositeAggregatorFieldsDescriptor()
  {
    //the default construct is private, walk around.
    super(Collections.<String, Type>emptyMap());
  }
  
  public CompositeAggregatorFieldsDescriptor(Map<String, Type> fieldToType)
  {
    super(fieldToType);
  }
  public CompositeAggregatorFieldsDescriptor(Map<String, Type> fieldToType,
      Map<String, Serde> fieldToSerdeObject)
  {
    super(fieldToType, fieldToSerdeObject);
  }
  
  public CompositeAggregatorFieldsDescriptor(Map<String, Type> fieldToType,
      Map<String, Serde> fieldToSerdeObject,
      SerdeObjectPayloadFix serdePayloadFix)
  {
    super(fieldToType, fieldToSerdeObject, serdePayloadFix);
  }

  public Serde[] getSerdes()
  {
    Serde[] serdes = super.getSerdes();
    if(serdes != null)
      return serdes;
    
    final int fieldSize = getFieldList().size();
    if(fieldSize > DEFAULT_COMPOSITE_SERDES.length)
    {
      //expand size
      createDefaultCompositeSerdes( DEFAULT_COMPOSITE_SERDES_SIZE * (fieldSize/DEFAULT_COMPOSITE_SERDES_SIZE + (fieldSize%DEFAULT_COMPOSITE_SERDES_SIZE == 0 ? 0 : 1)));
    }
    return DEFAULT_COMPOSITE_SERDES;
  }

  protected static void createDefaultCompositeSerdes(int size)
  {
    DEFAULT_COMPOSITE_SERDES = new Serde[size];
    for(int index = 0; index < DEFAULT_COMPOSITE_SERDES.length; ++index)
    {
      DEFAULT_COMPOSITE_SERDES[index] = SerdeMapPrimitive.INSTANCE;
    }
  }
}
