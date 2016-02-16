/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions;

import java.io.Serializable;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.gpo.GPOUtils.IndexSubset;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.datatorrent.lib.dimensions.aggregator.AbstractCompositeAggregator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * This is the base class for a generic single schema dimensions computation operator. A single
 * schema dimensions computation operator performs dimensions computation on inputs using a
 * predefined {@link DimensionalConfigurationSchema}. The way in which dimensions computation
 * is performed on these inputs is the following:
 * <ol>
 * <li>A {@link DimensionalConfigurationSchema} is set on the operator which specifies the
 * keys, values, dimension combinations, and aggregations to perform over the dimension combinations.</li>
 * <li>The aggregators are set on the operator as an {@link AggregatorRegistry}</li>
 * <li>An event is received.</li>
 * <li>The event is converted into an {@link InputEvent} via the {@link #convert} method.</li>
 * <li>The {@link InputEvent} is passed on to the {@link DimensionsComputation} operator, which
 * performs dimensions computation.</li>
 * <li>Aggregations are emitted by the operator as {@link Aggregate}s.</li>
 * </ol>
 *
 * @param <EVENT> The type of the input events on which to perform dimensions computation.
 * @since 3.1.0
 */
public abstract class AbstractDimensionsComputationFlexibleSingleSchema<EVENT> implements Operator
{
  /**
   * The default schema ID.
   */
  public static final int DEFAULT_SCHEMA_ID = 1;
  /**
   * This holds the JSON which defines the {@link DimensionalConfigurationSchema} to be used by this operator.
   */
  @NotNull
  private String configurationSchemaJSON;
  /**
   * The {@link DimensionalConfigurationSchema} to be used by this operator.
   * Note this should be kept NON-transient because some operators set this directly
   * without using a JSON configuration schema.
   */
  protected DimensionalConfigurationSchema configurationSchema;
  /**
   * The schemaID applied to {@link DimensionsEvent}s generated by this operator.
   */
  private int schemaID = DEFAULT_SCHEMA_ID;
  /**
   * This is the preexisting dimensions computation operator that is used to perform dimensions
   * computation underneath the hood.
   */
  private DimensionsComputation<InputEvent, Aggregate> dimensionsComputation;
  /**
   * This is the unifier used for dimensions computation.
   */
  private DimensionsComputationUnifierImpl<InputEvent, Aggregate> unifier;
  /**
   * The {@link AggregatorRegistry} to use for this dimensions computation operator.
   */
  protected AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  /**
   * This is a reused {@link InputEvent} object. Its purpose is to serve as a container for converted
   * inputs. This {@link InputEvent} should have all the keys and values defined in the schema extracted into
   * it. Additionally if time is specified in the schema, the value of time should be extracted into the key
   * of this schema.
   */
  protected InputEvent inputEvent;

  /**
   * The output port for the aggregates.
   */
  public final transient DefaultOutputPort<Aggregate> output = new DefaultOutputPort<Aggregate>()
  {
    @Override
    public Unifier<Aggregate> getUnifier()
    {
      unifier.setAggregators(createIncrementalAggregators());
      return unifier;
    }
  };

  /**
   * The input port which receives events to perform dimensions computation on.
   */
  public final transient DefaultInputPort<EVENT> input = new DefaultInputPort<EVENT>()
  {
    @Override
    public void process(EVENT tuple)
    {
      processInputEvent(tuple);
    }
  };

  public AbstractDimensionsComputationFlexibleSingleSchema()
  {
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void setup(OperatorContext context)
  {
    aggregatorRegistry.setup();

    if (configurationSchema == null) {
      configurationSchema = new DimensionalConfigurationSchema(configurationSchemaJSON,
          aggregatorRegistry);
    }

    IncrementalAggregator[] incrementalAggregatorArray = createIncrementalAggregators();

    dimensionsComputation = new DimensionsComputation<InputEvent, Aggregate>();
    dimensionsComputation.setIncrementalAggregators(incrementalAggregatorArray);

//bright: remove the composite aggregator need to compute in store.    
//    AbstractCompositeAggregator[] compositeAggregatorArray = createCompositeAggregators();
//    dimensionsComputation.setCompositeAggregators(compositeAggregatorArray);
    

    Sink<Aggregate> sink = new Sink<Aggregate>()
    {

      @Override
      public void put(Aggregate tuple)
      {
        output.emit(tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }
    };

    dimensionsComputation.output.setSink((Sink)sink);
    dimensionsComputation.setup(context);

    createInputEvent();
  }

  private void createInputEvent()
  {
    inputEvent = new InputEvent(
        new EventKey(0,
            0,
            0,
            new GPOMutable(this.configurationSchema.getKeyDescriptorWithTime())),
        new GPOMutable(this.configurationSchema.getInputValuesDescriptor()));
  }

  /**
   * The composite aggregators depended on the embed aggregator.
   * fulfill the information to configurationSchema
   * The embed aggregators could add dimension combination.
   */
  //bright: remove this function as embed aggerator already fulfilled when doing configure. 
  protected void fulfillCompositeEmbedAggregators()
  {
    if(true)
      throw new RuntimeException("not used.");
    
    int numCompositeAggregators = 0;

    FieldsDescriptor masterKeyFieldsDescriptor = configurationSchema.getKeyDescriptorWithTime();
    List<FieldsDescriptor> keyFieldsDescriptors = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor();

    //Compute the number of aggregators to create
    for (int dimensionsDescriptorID = 0;
        dimensionsDescriptorID < configurationSchema.getDimensionsDescriptorIDToCompositeAggregatorIDs().size();
        dimensionsDescriptorID++) {
      IntArrayList aggIDList = configurationSchema.getDimensionsDescriptorIDToCompositeAggregatorIDs().get(
          dimensionsDescriptorID);
      numCompositeAggregators += aggIDList.size();
    }

    AbstractCompositeAggregator[] aggregatorArray = new AbstractCompositeAggregator[numCompositeAggregators];
    int compositeAggregatorIndex = 0;

    for (int dimensionsDescriptorID = 0;
        dimensionsDescriptorID < keyFieldsDescriptors.size();
        dimensionsDescriptorID++) {
      // the combination of embed aggregator is the combination of composite aggreagtor's combination and sub-combination
    
    }
  
  }
  
  /**
   * This is a helper method which initializes internal data structures for the operator and
   * creates the array of aggregators which are set on the {@link DimensionsComputation} operator
   * (which is used internally to perform dimensions computation), and the {@link DimensionsComputationUnifierImpl}.
   *
   * @return The aggregators to be set on the unifier and internal {@link DimensionsComputation} operator.
   */
  private IncrementalAggregator[] createIncrementalAggregators()
  {
    //Num incremental aggregators
    int numIncrementalAggregators = 0;

    FieldsDescriptor masterKeyFieldsDescriptor = configurationSchema.getKeyDescriptorWithTime();
    List<FieldsDescriptor> keyFieldsDescriptors = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor();

    //Compute the number of aggregators to create
    for (int dimensionsDescriptorID = 0;
        dimensionsDescriptorID < configurationSchema.getDimensionsDescriptorIDToIncrementalAggregatorIDs().size();
        dimensionsDescriptorID++) {
      IntArrayList aggIDList = configurationSchema.getDimensionsDescriptorIDToIncrementalAggregatorIDs().get(
          dimensionsDescriptorID);
      numIncrementalAggregators += aggIDList.size();
    }

    IncrementalAggregator[] aggregatorArray = new IncrementalAggregator[numIncrementalAggregators];
    int incrementalAggregatorIndex = 0;

    for (int dimensionsDescriptorID = 0;
        dimensionsDescriptorID < keyFieldsDescriptors.size();
        dimensionsDescriptorID++) {
      //Create the conversion context for the conversion.
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(dimensionsDescriptorID);
      Int2ObjectMap<FieldsDescriptor> map = configurationSchema
          .getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().get(dimensionsDescriptorID);
      Int2ObjectMap<FieldsDescriptor> mapOutput = configurationSchema
          .getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionsDescriptorID);
      IntArrayList aggIDList = configurationSchema
          .getDimensionsDescriptorIDToIncrementalAggregatorIDs().get(dimensionsDescriptorID);
      DimensionsDescriptor dd = configurationSchema
          .getDimensionsDescriptorIDToDimensionsDescriptor().get(dimensionsDescriptorID);

      for (int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++, incrementalAggregatorIndex++) {
        int aggID = aggIDList.get(aggIDIndex);

        DimensionsConversionContext conversionContext = new DimensionsConversionContext();
        IndexSubset indexSubsetKey = GPOUtils.computeSubIndices(keyFieldsDescriptor, masterKeyFieldsDescriptor);
        IndexSubset indexSubsetAggregate = GPOUtils
            .computeSubIndices(configurationSchema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor()
            .get(dimensionsDescriptorID).get(aggID), configurationSchema.getInputValuesDescriptor());

        conversionContext.schemaID = schemaID;
        conversionContext.dimensionsDescriptorID = dimensionsDescriptorID;
        conversionContext.aggregatorID = aggID;
        conversionContext.customTimeBucketRegistry = configurationSchema.getCustomTimeBucketRegistry();
        conversionContext.dd = dd;
        conversionContext.keyDescriptor = keyFieldsDescriptor;
        conversionContext.aggregateDescriptor = map.get(aggID);
        conversionContext.aggregateDescriptor = mapOutput.get(aggID);

        {
          List<String> fields = masterKeyFieldsDescriptor.getTypeToFields().get(
              DimensionsDescriptor.DIMENSION_TIME_TYPE);

          if (fields == null) {
            conversionContext.inputTimestampIndex = -1;
          } else {
            conversionContext.inputTimestampIndex = fields.indexOf(DimensionsDescriptor.DIMENSION_TIME);
          }
        }

        {
          List<String> fields = keyFieldsDescriptor.getTypeToFields().get(
              DimensionsDescriptor.DIMENSION_TIME_BUCKET_TYPE);

          if (fields == null) {
            conversionContext.outputTimebucketIndex = -1;
          } else {
            conversionContext.outputTimebucketIndex = fields.indexOf(DimensionsDescriptor.DIMENSION_TIME_BUCKET);
          }
        }

        conversionContext.indexSubsetKeys = indexSubsetKey;
        conversionContext.indexSubsetAggregates = indexSubsetAggregate;

        IncrementalAggregator aggregator;

        try {
          aggregator = this.aggregatorRegistry.getIncrementalAggregatorIDToAggregator().get(aggID).getClass()
              .newInstance();
        } catch (InstantiationException ex) {
          throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
          throw new RuntimeException(ex);
        }

        aggregator.setDimensionsConversionContext(conversionContext);
        aggregatorArray[incrementalAggregatorIndex] = aggregator;
      }
    }

    return aggregatorArray;
  }

  
  protected AbstractCompositeAggregator[] createCompositeAggregators()
  {
    //Num of compositeAggre aggregators
    int numCompositeAggregators = 0;

    FieldsDescriptor masterKeyFieldsDescriptor = configurationSchema.getKeyDescriptorWithTime();
    List<FieldsDescriptor> keyFieldsDescriptors = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor();

    //Compute the number of aggregators to create
    for (int dimensionsDescriptorID = 0;
        dimensionsDescriptorID < configurationSchema.getDimensionsDescriptorIDToCompositeAggregatorIDs().size();
        dimensionsDescriptorID++) {
      IntArrayList aggIDList = configurationSchema.getDimensionsDescriptorIDToCompositeAggregatorIDs().get(
          dimensionsDescriptorID);
      numCompositeAggregators += aggIDList.size();
    }

    AbstractCompositeAggregator[] aggregatorArray = new AbstractCompositeAggregator[numCompositeAggregators];
    int compositeAggregatorIndex = 0;

    for (int dimensionsDescriptorID = 0;
        dimensionsDescriptorID < keyFieldsDescriptors.size();
        dimensionsDescriptorID++) {
      //Create the conversion context for the conversion.
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(dimensionsDescriptorID);
      Int2ObjectMap<FieldsDescriptor> map = configurationSchema
          .getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().get(dimensionsDescriptorID);
      Int2ObjectMap<FieldsDescriptor> mapOutput = configurationSchema
          .getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionsDescriptorID);
      IntArrayList aggIDList = configurationSchema
          .getDimensionsDescriptorIDToCompositeAggregatorIDs().get(dimensionsDescriptorID);
      DimensionsDescriptor dd = configurationSchema
          .getDimensionsDescriptorIDToDimensionsDescriptor().get(dimensionsDescriptorID);

      for (int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++, compositeAggregatorIndex++) {
        int aggID = aggIDList.get(aggIDIndex);

        //bright: TODO: need to find the embed incremental aggregator
        
        DimensionsConversionContext conversionContext = new DimensionsConversionContext();
        IndexSubset indexSubsetKey = GPOUtils.computeSubIndices(keyFieldsDescriptor, masterKeyFieldsDescriptor);
        IndexSubset indexSubsetAggregate = GPOUtils
            .computeSubIndices(configurationSchema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor()
            .get(dimensionsDescriptorID).get(aggID), configurationSchema.getInputValuesDescriptor());

        conversionContext.schemaID = schemaID;
        conversionContext.dimensionsDescriptorID = dimensionsDescriptorID;
        conversionContext.aggregatorID = aggID;
        conversionContext.customTimeBucketRegistry = configurationSchema.getCustomTimeBucketRegistry();
        conversionContext.dd = dd;
        conversionContext.keyDescriptor = keyFieldsDescriptor;
        //conversionContext.aggregateDescriptor = map.get(aggID);
        conversionContext.aggregateDescriptor = mapOutput.get(aggID);

        {
          List<String> fields = masterKeyFieldsDescriptor.getTypeToFields().get(
              DimensionsDescriptor.DIMENSION_TIME_TYPE);

          if (fields == null) {
            conversionContext.inputTimestampIndex = -1;
          } else {
            conversionContext.inputTimestampIndex = fields.indexOf(DimensionsDescriptor.DIMENSION_TIME);
          }
        }

        {
          List<String> fields = keyFieldsDescriptor.getTypeToFields().get(
              DimensionsDescriptor.DIMENSION_TIME_BUCKET_TYPE);

          if (fields == null) {
            conversionContext.outputTimebucketIndex = -1;
          } else {
            conversionContext.outputTimebucketIndex = fields.indexOf(DimensionsDescriptor.DIMENSION_TIME_BUCKET);
          }
        }

        conversionContext.indexSubsetKeys = indexSubsetKey;
        conversionContext.indexSubsetAggregates = indexSubsetAggregate;

        AbstractCompositeAggregator<Object> aggregator = null; //aggregatorRegistry.getCompositeAggregatorIDToAggregator().get(aggID).clone();
        aggregator.setDimensionsConversionContext(conversionContext);
        aggregatorArray[compositeAggregatorIndex] = aggregator;
      }
    }

    return aggregatorArray;
  }
  
  @Override
  public void beginWindow(long windowId)
  {
    dimensionsComputation.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    dimensionsComputation.endWindow();
  }

  @Override
  public void teardown()
  {
    dimensionsComputation.teardown();
  }

  public void processInputEvent(EVENT event)
  {
    convert(inputEvent, event);
    dimensionsComputation.data.put(inputEvent);

    if (inputEvent.used) {
      createInputEvent();
    }
  }

  /**
   * This method converts a given event received by the operator into an {@link InputEvent}
   * object. All of the keys and values defined in this operators {@link DimensionalConfigurationSchema}
   * should be packaged into the {@link InputEvent} received by this method, including the time
   * field if it is specified as a key.
   *
   * @param inputEvent The {@link InputEvent} to convert received events into.
   * @param event      The event to unpack into the given {@link InputEvent}.
   */
  public abstract void convert(InputEvent inputEvent, EVENT event);

  /**
   * Gets the {@link AggregatorRegistry} for this operator.
   *
   * @return The {@link AggregatorRegistry} for this operator.
   */
  protected AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * Sets the {@link AggregatorRegistry} for this operator.
   *
   * @param aggregatorRegistry The {@link AggregatorRegistry} for this operator.
   */
  public void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  /**
   * Gets the JSON specifying the {@link com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema} for this
   * operator.
   *
   * @return The JSON specifying the {@link DimensionalConfigurationSchema} for this operator.
   */
  public String getConfigurationSchemaJSON()
  {
    return configurationSchemaJSON;
  }

  /**
   * Sets the JSON specifying the {@link DimensionalConfigurationSchema} for this operator.
   *
   * @param configurationSchemaJSON The JSON specifying the {@link DimensionalConfigurationSchema} for
   *                                this operator.
   */
  public void setConfigurationSchemaJSON(String configurationSchemaJSON)
  {
    this.configurationSchemaJSON = configurationSchemaJSON;
  }

  /**
   * Gets the unifier set on this operator.
   *
   * @return The unifier set on this operator.
   */
  public DimensionsComputationUnifierImpl<InputEvent, Aggregate> getUnifier()
  {
    return unifier;
  }

  /**
   * Sets the unifier on this operator.
   *
   * @param unifier The unifier set on this operator.
   */
  public void setUnifier(DimensionsComputationUnifierImpl<InputEvent, Aggregate> unifier)
  {
    this.unifier = unifier;
  }

  /**
   * @return the schemaID
   */
  public int getSchemaID()
  {
    return schemaID;
  }

  /**
   * @param schemaID the schemaID to set
   */
  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  /**
   * This is a context object used to convert {@link InputEvent}s into aggregates in {@link IncrementalAggregator}s.
   */
  public static class DimensionsConversionContext implements Serializable
  {
    private static final long serialVersionUID = 201506151157L;

    public CustomTimeBucketRegistry customTimeBucketRegistry;
    /**
     * The schema ID for
     * {@link Aggregate}s emitted by the {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s
     * holding this context.
     */
    public int schemaID;
    /**
     * The dimensionsDescriptor ID for {@link Aggregate}s emitted by the
     * {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s
     * holding this context.
     */
    public int dimensionsDescriptorID;
    /**
     * The aggregator ID for {@link Aggregate}s emitted by the
     * {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s holding this context.
     */
    public int aggregatorID;
    /**
     * The {@link DimensionsDescriptor} corresponding to the given dimension descriptor id.
     */
    public DimensionsDescriptor dd;
    /**
     * The
     * {@link FieldsDescriptor} for the aggregate of the {@link Aggregate}s emitted by the
     * {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s holding this context object.
     */
    public FieldsDescriptor aggregateDescriptor;
    /**
     * The
     * {@link FieldsDescriptor} for the key of the {@link Aggregate}s emitted by the
     * {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s holding this context object.
     */
    public FieldsDescriptor keyDescriptor;
    /**
     * The index of the timestamp field within the key of
     * {@link InputEvent}s received by the {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s
     * holding this context object. This is -1 if the {@link InputEvent} key has no timestamp.
     */
    public int inputTimestampIndex;
    /**
     * The index of the timestamp field within the key of
     * {@link Aggregate}s emitted by the {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s
     * holding this context object. This is -1 if the {@link Aggregate}'s key has no timestamp.
     */
    public int outputTimestampIndex;
    /**
     * The index of the time bucket field within the key of
     * {@link Aggregate}s emitted by the {@link com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator}s
     * holding this context object. This is -1 if the {@link Aggregate}'s key has no timebucket.
     */
    public int outputTimebucketIndex;
    /**
     * The {@link IndexSubset} object that is used to extract key values from {@link InputEvent}s
     * received by this aggregator.
     */
    public IndexSubset indexSubsetKeys;
    /**
     * The {@link IndexSubset} object that is used to extract aggregate values from {@link InputEvent}s
     * received by this aggregator.
     */
    public IndexSubset indexSubsetAggregates;

    /**
     * Constructor for creating conversion context.
     */
    public DimensionsConversionContext()
    {
      //Do nothing.
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDimensionsComputationFlexibleSingleSchema.class);
}
