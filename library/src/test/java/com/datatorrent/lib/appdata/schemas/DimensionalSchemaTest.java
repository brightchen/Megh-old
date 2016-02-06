/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.dimensions.aggregator.AggregatorBottom;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.AggregatorTop;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.datatorrent.lib.dimensions.aggregator.OTFAggregator;
import com.datatorrent.lib.dimensions.aggregator.AbstractCompositeAggregator;
import com.datatorrent.lib.dimensions.aggregator.AbstractTopBottomAggregator;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class DimensionalSchemaTest
{
  private static final String FIELD_TAGS = "tags";

  public DimensionalSchemaTest()
  {
  }

  @Before
  public void initialize()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void noEnumsTest()
  {
    //Test if creating schema with no enums works
    DimensionalConfigurationSchema des =
    new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaNoEnums.json"),
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
  }

  @Test
  public void noTimeTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchemaNoTime.json");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("revenue:SUM", "double");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>)new HashSet<String>(),
                                                                     Sets.newHashSet("location"),
                                                                     Sets.newHashSet("advertiser"),
                                                                     Sets.newHashSet("publisher"),
                                                                     Sets.newHashSet("location", "advertiser"),
                                                                     Sets.newHashSet("location", "publisher"),
                                                                     Sets.newHashSet("advertiser", "publisher"),
                                                                     Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema,
                       Lists.newArrayList(TimeBucket.ALL.getText()),
                       Lists.newArrayList("publisher", "advertiser", "location"),
                       Lists.newArrayList("string", "string", "string"),
                       valueToType,
                       dimensionCombinationsList);
  }

  @Test
  public void globalValueTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchema.json");

    List<String> timeBuckets = Lists.newArrayList("1m", "1h", "1d");
    List<String> keyNames = Lists.newArrayList("publisher", "advertiser", "location");
    List<String> keyTypes = Lists.newArrayList("string", "string", "string");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("revenue:SUM", "double");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>) new HashSet<String>(),
                                                                     Sets.newHashSet("location"),
                                                                     Sets.newHashSet("advertiser"),
                                                                     Sets.newHashSet("publisher"),
                                                                     Sets.newHashSet("location", "advertiser"),
                                                                     Sets.newHashSet("location", "publisher"),
                                                                     Sets.newHashSet("advertiser", "publisher"),
                                                                     Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema,
                       timeBuckets,
                       keyNames,
                       keyTypes,
                       valueToType,
                       dimensionCombinationsList);
  }

  @Test
  public void additionalValueTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchemaAdditional.json");

    List<String> timeBuckets = Lists.newArrayList("1m", "1h", "1d");
    List<String> keyNames = Lists.newArrayList("publisher", "advertiser", "location");
    List<String> keyTypes = Lists.newArrayList("string", "string", "string");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("impressions:COUNT", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("clicks:COUNT", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("cost:COUNT", "long");
    valueToType.put("revenue:SUM", "double");
    valueToType.put("revenue:COUNT", "long");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>) new HashSet<String>(),
                                                                     Sets.newHashSet("location"),
                                                                     Sets.newHashSet("advertiser"),
                                                                     Sets.newHashSet("publisher"),
                                                                     Sets.newHashSet("location", "advertiser"),
                                                                     Sets.newHashSet("location", "publisher"),
                                                                     Sets.newHashSet("advertiser", "publisher"),
                                                                     Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema,
                       timeBuckets,
                       keyNames,
                       keyTypes,
                       valueToType,
                       dimensionCombinationsList);

    Map<String, String> additionalValueMap = Maps.newHashMap();
    additionalValueMap.put("impressions:MAX", "long");
    additionalValueMap.put("clicks:MAX", "long");
    additionalValueMap.put("cost:MAX", "double");
    additionalValueMap.put("revenue:MAX", "double");
    additionalValueMap.put("impressions:MIN", "long");
    additionalValueMap.put("clicks:MIN", "long");
    additionalValueMap.put("cost:MIN", "double");
    additionalValueMap.put("revenue:MIN", "double");

    @SuppressWarnings("unchecked")
    List<Map<String, String>> additionalValuesList = Lists.newArrayList((Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                additionalValueMap,
                                                                additionalValueMap,
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>());

    JSONObject data = new JSONObject(resultSchema).getJSONArray("data").getJSONObject(0);
    JSONArray dimensions = data.getJSONArray("dimensions");

    for(int index = 0;
        index < dimensions.length();
        index++) {
      JSONObject combination = dimensions.getJSONObject(index);

      Map<String, String> tempAdditionalValueMap = additionalValuesList.get(index);
      Assert.assertEquals(tempAdditionalValueMap.isEmpty(), !combination.has("additionalValues"));

      Set<String> additionalValueSet = Sets.newHashSet();

      if(tempAdditionalValueMap.isEmpty()) {
        continue;
      }

      JSONArray additionalValues = combination.getJSONArray("additionalValues");

      LOG.debug("additionalValues {}", additionalValues);

      for(int aIndex = 0;
          aIndex < additionalValues.length();
          aIndex++) {
        JSONObject additionalValue = additionalValues.getJSONObject(aIndex);

        String valueName = additionalValue.getString("name");
        String valueType = additionalValue.getString("type");

        String expectedValueType = tempAdditionalValueMap.get(valueName);

        Assert.assertTrue("Duplicate value " + valueName, additionalValueSet.add(valueName));
        Assert.assertTrue("Invalid value " + valueName, expectedValueType != null);
        Assert.assertEquals(expectedValueType, valueType);
      }
    }
  }

  @Test
  public void enumValUpdateTest() throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    DimensionalSchema dimensional = new DimensionalSchema(
                                    new DimensionalConfigurationSchema(eventSchemaJSON,
                                                               AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    Map<String, List<Object>> replacementEnums = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    List<Object> publisherEnumList = ((List<Object>) ((List) Lists.newArrayList("google", "twitter")));
    @SuppressWarnings("unchecked")
    List<Object> advertiserEnumList = ((List<Object>) ((List) Lists.newArrayList("google", "twitter")));
    @SuppressWarnings("unchecked")
    List<Object> locationEnumList = ((List<Object>) ((List) Lists.newArrayList("google", "twitter")));

    replacementEnums.put("publisher", publisherEnumList);
    replacementEnums.put("advertiser", advertiserEnumList);
    replacementEnums.put("location", locationEnumList);

    dimensional.setEnumsList(replacementEnums);

    String schemaJSON = dimensional.getSchemaJSON();

    JSONObject schema = new JSONObject(schemaJSON);
    JSONArray keys = schema.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS);

    Map<String, List<Object>> newEnums = Maps.newHashMap();

    for(int keyIndex = 0;
        keyIndex < keys.length();
        keyIndex++) {
      JSONObject keyData = keys.getJSONObject(keyIndex);
      String name = keyData.getString(DimensionalConfigurationSchema.FIELD_KEYS_NAME);
      JSONArray enumValues = keyData.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS_ENUMVALUES);
      List<Object> enumList = Lists.newArrayList();

      for(int enumIndex = 0;
          enumIndex < enumValues.length();
          enumIndex++) {
        enumList.add(enumValues.get(enumIndex));
      }

      newEnums.put(name, enumList);
    }

    Assert.assertEquals(replacementEnums, newEnums);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void enumValUpdateTestComparable() throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    DimensionalSchema dimensional = new DimensionalSchema(
                                    new DimensionalConfigurationSchema(eventSchemaJSON,
                                                               AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    Map<String, Set<Comparable>> replacementEnums = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    Set<Comparable> publisherEnumList = ((Set<Comparable>) ((Set) Sets.newHashSet("b", "c", "a")));
    @SuppressWarnings("unchecked")
    Set<Comparable> advertiserEnumList = ((Set<Comparable>) ((Set) Sets.newHashSet("b", "c", "a")));
    @SuppressWarnings("unchecked")
    Set<Comparable> locationEnumList = ((Set<Comparable>) ((Set) Sets.newHashSet("b", "c", "a")));

    replacementEnums.put("publisher", publisherEnumList);
    replacementEnums.put("advertiser", advertiserEnumList);
    replacementEnums.put("location", locationEnumList);

    Map<String, List<Comparable>> expectedOutput = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    List<Comparable> publisherEnumSortedList = (List<Comparable>) ((List) Lists.newArrayList("a", "b", "c"));
    @SuppressWarnings("unchecked")
    List<Comparable> advertiserEnumSortedList = (List<Comparable>) ((List) Lists.newArrayList("a", "b", "c"));
    @SuppressWarnings("unchecked")
    List<Comparable> locationEnumSortedList = (List<Comparable>) ((List) Lists.newArrayList("a", "b", "c"));

    expectedOutput.put("publisher", publisherEnumSortedList);
    expectedOutput.put("advertiser", advertiserEnumSortedList);
    expectedOutput.put("location", locationEnumSortedList);

    dimensional.setEnumsSetComparable(replacementEnums);

    String schemaJSON = dimensional.getSchemaJSON();

    JSONObject schema = new JSONObject(schemaJSON);
    JSONArray keys = schema.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS);

    Map<String, List<Comparable>> newEnums = Maps.newHashMap();

    for(int keyIndex = 0;
        keyIndex < keys.length();
        keyIndex++) {
      JSONObject keyData = keys.getJSONObject(keyIndex);
      String name = keyData.getString(DimensionalConfigurationSchema.FIELD_KEYS_NAME);
      JSONArray enumValues = keyData.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS_ENUMVALUES);
      List<Comparable> enumList = Lists.newArrayList();

      for(int enumIndex = 0;
          enumIndex < enumValues.length();
          enumIndex++) {
        enumList.add((Comparable) enumValues.get(enumIndex));
      }

      newEnums.put(name, enumList);
    }

    Assert.assertEquals(expectedOutput, newEnums);
  }

  @Test
  public void testSchemaTags() throws Exception
  {
    List<String> expectedTags = Lists.newArrayList("geo", "bullet");
    List<String> expectedKeyTags = Lists.newArrayList("geo.location");
    List<String> expectedValueTagsLat = Lists.newArrayList("geo.lattitude");
    List<String> expectedValueTagsLong = Lists.newArrayList("geo.longitude");

    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaTags.json");
    DimensionalSchema dimensional = new DimensionalSchema(
      new DimensionalConfigurationSchema(eventSchemaJSON,
                                         AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    String schemaJSON = dimensional.getSchemaJSON();

    JSONObject jo = new JSONObject(schemaJSON);
    List<String> tags = getStringList(jo.getJSONArray(FIELD_TAGS));
    Assert.assertEquals(expectedTags, tags);

    JSONArray keys = jo.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS);

    List<String> keyTags = null;

    for (int keyIndex = 0; keyIndex < keys.length(); keyIndex++) {
      JSONObject key = keys.getJSONObject(keyIndex);

      if (!key.has(FIELD_TAGS)) {
        continue;
      }

      Assert.assertEquals("location", key.get(DimensionalConfigurationSchema.FIELD_KEYS_NAME));
      keyTags = getStringList(key.getJSONArray(FIELD_TAGS));
    }

    Assert.assertTrue("No tags found for any key", keyTags != null);
    Assert.assertEquals(expectedKeyTags, keyTags);

    JSONArray values = jo.getJSONArray(DimensionalConfigurationSchema.FIELD_VALUES);

    boolean valueTagsLat = false;
    boolean valueTagsLong = false;

    for (int valueIndex = 0; valueIndex < values.length(); valueIndex++) {
      JSONObject value = values.getJSONObject(valueIndex);

      if (!value.has(FIELD_TAGS)) {
        continue;
      }

      String valueName = value.getString(DimensionalConfigurationSchema.FIELD_VALUES_NAME);
      List<String> valueTags = getStringList(value.getJSONArray(FIELD_TAGS));

      LOG.debug("value name: {}", valueName);

      if (valueName.startsWith("impressions")) {
        Assert.assertEquals(expectedValueTagsLat, valueTags);
        valueTagsLat = true;
      } else if (valueName.startsWith("clicks")) {
        Assert.assertEquals(expectedValueTagsLong, valueTags);
        valueTagsLong = true;
      } else {
        Assert.fail("There should be no tags for " + valueName);
      }
    }

    Assert.assertTrue("No tags found for impressions", valueTagsLat);
    Assert.assertTrue("No tags found for clicks", valueTagsLong);
  }

  /**
   * test the schema of aggregator with embed schema and property
   * @throws Exception
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSchemaComposite() throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaComposite.json");
    final DimensionalConfigurationSchema dimensionConfigSchema = new DimensionalConfigurationSchema(eventSchemaJSON, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
    final DimensionalSchema dimensional = new DimensionalSchema(dimensionConfigSchema);

    Map<String, AbstractTopBottomAggregator<Object>> compsiteNameToAggregator = dimensional.getAggregatorRegistry().getNameToTopBottomAggregator();
    Map<String, IncrementalAggregator> incrementalNameToAggregator = dimensional.getAggregatorRegistry().getNameToIncrementalAggregator();
    Map<String, OTFAggregator> otfNameToAggregator = dimensional.getAggregatorRegistry().getNameToOTFAggregators();
    
    //verify the name to aggregator
    //expected name to aggregator
    Map<String, AbstractTopBottomAggregator<Object>> expectedNameToAggregator = Maps.newHashMap();
    final String[] combination_location = new String[]{"location"};
    final String[] combination_advertiser = new String[]{"advertiser"};
    final String[] combination_location_publisher = new String[]{"location", "publisher"};
    expectedNameToAggregator.put("TOPN-SUM-10_location", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
        .withEmbedAggregatorName("SUM").withSubCombinations(combination_location));
    expectedNameToAggregator.put("BOTTOMN-AVG-20_location", new AggregatorBottom().withCount(20).withEmbedAggregator(otfNameToAggregator.get("AVG"))
        .withEmbedAggregatorName("AVG").withSubCombinations(combination_location));
    expectedNameToAggregator.put("TOPN-SUM-10_location_publisher", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
        .withEmbedAggregatorName("SUM").withSubCombinations(combination_location_publisher));
    expectedNameToAggregator.put("TOPN-SUM-10_advertiser", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
        .withEmbedAggregatorName("SUM").withSubCombinations(combination_advertiser));
    expectedNameToAggregator.put("TOPN-COUNT-10_location", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("COUNT"))
        .withEmbedAggregatorName("COUNT").withSubCombinations(combination_location));
    expectedNameToAggregator.put("BOTTOMN-AVG-10_location", new AggregatorBottom().withCount(10).withEmbedAggregator(otfNameToAggregator.get("AVG"))
        .withEmbedAggregatorName("AVG").withSubCombinations(combination_location));
    expectedNameToAggregator.put("BOTTOMN-SUM-10_location", new AggregatorBottom().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
        .withEmbedAggregatorName("SUM").withSubCombinations(combination_location));
    
    MapDifference difference = Maps.difference(compsiteNameToAggregator, expectedNameToAggregator);
    Assert.assertTrue("Generated Composit Aggregators are not same as expected.\n" + difference.toString(), difference.areEqual()); 
    
    
    //verify AggregateNameToFD
    //implicit added combinations
    Set<Set<String>> implicitAddedCombinations = Sets.newHashSet();
    {
      implicitAddedCombinations.add(Sets.newHashSet("location", "advertiser"));
      implicitAddedCombinations.add(Sets.newHashSet("location", "publiser"));
      implicitAddedCombinations.add(Sets.newHashSet("location", "advertiser", "publiser"));
    }
    
    //
    List<Map<String, FieldsDescriptor>> ddIDToAggregatorToDesc = dimensionConfigSchema.getDimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor();
    List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggregatorIDToInputDesc = dimensionConfigSchema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor();
    List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggregatorIDToOutputDesc = dimensionConfigSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor();
    List<IntArrayList> ddIDToIncrementalAggregatorIDs = dimensionConfigSchema.getDimensionsDescriptorIDToIncrementalAggregatorIDs();
    List<IntArrayList> ddIDToCompositeAggregatorIDs = dimensionConfigSchema.getDimensionsDescriptorIDToCompositeAggregatorIDs();
    
    
    //size
    final int expectedDdIDNum = (3 + implicitAddedCombinations.size()) * 2;
    Assert.assertTrue(ddIDToAggregatorToDesc.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToAggregatorIDToInputDesc.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToAggregatorIDToOutputDesc.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToIncrementalAggregatorIDs.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToCompositeAggregatorIDs.size() == expectedDdIDNum);
    
    //expectedAggregateNameToFD
    Map<String, FieldsDescriptor> expectedCommonAggregateNameToFD = Maps.newHashMap();
    //common
    {
      //TOPN-SUM-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("impressions", Type.LONG);
      fieldToType.put("clicks", Type.LONG);
      expectedCommonAggregateNameToFD.put("TOPN-SUM-10_location", new FieldsDescriptor(fieldToType));
    }
    {
      //BOTTOMN-AVG-20_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("clicks", Type.LONG);
      expectedCommonAggregateNameToFD.put("BOTTOMN-AVG-20_location", new FieldsDescriptor(fieldToType));
    }
    
    //specific
    //first
    Map<String, FieldsDescriptor> expectedFirstCombinationAggregateNameToFD = Maps.newHashMap();
    {
      //TOPN-SUM-10_location_publisher
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("impressions", Type.LONG);
      expectedFirstCombinationAggregateNameToFD.put("TOPN-SUM-10_location_publisher", new FieldsDescriptor(fieldToType));
    }
    
    //second
    Map<String, FieldsDescriptor> expectedSecondCombinationAggregateNameToFD = Maps.newHashMap();
    {
      //TOPN-SUM-10_advertiser
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("impressions", Type.LONG);
      expectedSecondCombinationAggregateNameToFD.put("TOPN-SUM-10_advertiser", new FieldsDescriptor(fieldToType));
    }
    
    Map<String, FieldsDescriptor> expectedThirdCombinationAggregateNameToFD = Maps.newHashMap();
    {
      //TOPN-SUM-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", Type.DOUBLE);
      expectedThirdCombinationAggregateNameToFD.put("TOPN-SUM-10_location", new FieldsDescriptor(fieldToType));
    }
    {
      //TOPN-COUNT-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", Type.DOUBLE);
      expectedThirdCombinationAggregateNameToFD.put("TOPN-COUNT-10_location", new FieldsDescriptor(fieldToType));
    }
    {
      //BOTTOMN-SUM-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", Type.DOUBLE);
      expectedThirdCombinationAggregateNameToFD.put("BOTTOMN-SUM-10_location", new FieldsDescriptor(fieldToType));
    }
    {
      //BOTTOMN-AVG-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", Type.DOUBLE);
      expectedThirdCombinationAggregateNameToFD.put("BOTTOMN-AVG-10_location", new FieldsDescriptor(fieldToType));
    }
   
    // put common
    Map<String, FieldsDescriptor> commonAggregateNameToFD = Maps.newHashMap();
    commonAggregateNameToFD.put("TOPN-SUM-10_location", expectedCommonAggregateNameToFD.get("TOPN-SUM-10_location"));
    commonAggregateNameToFD.put("BOTTOMN-AVG-20_location", expectedCommonAggregateNameToFD.get("BOTTOMN-AVG-20_location"));
    
    List<Map<String, FieldsDescriptor>> expectedDdIDToAggregatorToDesc = Lists.newArrayListWithCapacity(8);
    {
      //first/second
      merge(expectedFirstCombinationAggregateNameToFD, commonAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(0, expectedFirstCombinationAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(1, expectedFirstCombinationAggregateNameToFD);
    }
    
    {
      //third/forth
      merge(expectedSecondCombinationAggregateNameToFD, commonAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(2, expectedSecondCombinationAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(3, expectedSecondCombinationAggregateNameToFD);
    }
        
    {
      //fifth/sixth
      merge(expectedThirdCombinationAggregateNameToFD, commonAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(4, expectedThirdCombinationAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(5, expectedThirdCombinationAggregateNameToFD);
    }
    
    //others are empty
    for(int i=6; i<expectedDdIDNum; ++i)
      expectedDdIDToAggregatorToDesc.add(i, Collections.<String, FieldsDescriptor>emptyMap());
    
    for(int index=0; index<expectedDdIDNum; ++index)
    {
      MapDifference<String, FieldsDescriptor> diff = Maps.difference(expectedDdIDToAggregatorToDesc.get(index), ddIDToAggregatorToDesc.get(index));
      Assert.assertTrue(diff.toString(), diff.areEqual());
    }
    
//  
//  expectedNameToAggregator.put("TOPN-SUM-10_location", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
//      .withEmbedAggregatorName("SUM").withSubCombinations(combination_location));
//  expectedNameToAggregator.put("BOTTOMN-AVG-20_location", new AggregatorBottom().withCount(20).withEmbedAggregator(otfNameToAggregator.get("AVG"))
//      .withEmbedAggregatorName("AVG").withSubCombinations(combination_location));
//  expectedNameToAggregator.put("TOPN-SUM-10_location_publisher", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
//      .withEmbedAggregatorName("SUM").withSubCombinations(combination_location_publisher));
//  expectedNameToAggregator.put("TOPN-SUM-10_advertiser", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
//      .withEmbedAggregatorName("SUM").withSubCombinations(combination_advertiser));
//  expectedNameToAggregator.put("TOPN-COUNT-10_location", new AggregatorTop().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("COUNT"))
//      .withEmbedAggregatorName("COUNT").withSubCombinations(combination_location));
//  expectedNameToAggregator.put("BOTTOMN-AVG-10_location", new AggregatorBottom().withCount(10).withEmbedAggregator(otfNameToAggregator.get("AVG"))
//      .withEmbedAggregatorName("AVG").withSubCombinations(combination_location));
//  expectedNameToAggregator.put("BOTTOMN-SUM-10_location", new AggregatorBottom().withCount(10).withEmbedAggregator(incrementalNameToAggregator.get("SUM"))
 
    
    //
    // verify aggregatorIDs
    Map<String, Integer> incremantalAggregatorNameToID = dimensionConfigSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID();
    Map<String, Integer> compositeAggregatorNameToID = dimensionConfigSchema.getAggregatorRegistry().getTopBottomAggregatorNameToID();
    
    //common
    Set<Integer> expectedCommonAggregatorIds = Sets.newHashSet();
    expectedCommonAggregatorIds.add(incremantalAggregatorNameToID.get("SUM"));
    expectedCommonAggregatorIds.add(incremantalAggregatorNameToID.get("COUNT"));  //BOTTOMNN-AVG-20 depends
    expectedCommonAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_location"));
    expectedCommonAggregatorIds.add(compositeAggregatorNameToID.get("BOTTOMN-AVG-20_location"));
    
    Set<Integer> expectedFirstCombinationAggregatorIds = Sets.newHashSet();
    {
      expectedFirstCombinationAggregatorIds.addAll(expectedCommonAggregatorIds);
      expectedFirstCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_location_publisher"));
    }
    
    Set<Integer> expectedSecondCombinationAggregatorIds = Sets.newHashSet();
    {
      expectedSecondCombinationAggregatorIds.addAll(expectedCommonAggregatorIds);
      expectedSecondCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_advertiser"));
    }
    
    Set<Integer> expectedThirdCombinationAggregatorIds = Sets.newHashSet();
    {
      expectedThirdCombinationAggregatorIds.addAll(expectedCommonAggregatorIds);
      expectedThirdCombinationAggregatorIds.add(incremantalAggregatorNameToID.get("MIN"));
      expectedThirdCombinationAggregatorIds.add(incremantalAggregatorNameToID.get("MAX"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_location"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-COUNT-10_location"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("BOTTOMN-SUM-10_location"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("BOTTOMN-AVG-10_location"));
    }
        
    Set<Integer>[] expectedAllAggregatorIDSetArray = new Set[expectedDdIDNum];
    expectedAllAggregatorIDSetArray[0] = expectedAllAggregatorIDSetArray[1] = expectedFirstCombinationAggregatorIds;
    expectedAllAggregatorIDSetArray[2] = expectedAllAggregatorIDSetArray[3] = expectedSecondCombinationAggregatorIds;
    expectedAllAggregatorIDSetArray[4] = expectedAllAggregatorIDSetArray[5] = expectedThirdCombinationAggregatorIds;
    
    
    //implicit added dimension and incremental aggregators
    //there three combination of auto generated dimension
    
    Map<Set<String>, Set<Integer>> autoGeneratedKeysToAggregatorIds = Maps.newHashMap();
    final int sumId = incremantalAggregatorNameToID.get("SUM");
    final int countId = incremantalAggregatorNameToID.get("COUNT");
    
    //the two common composite TOP_SUM_10-location and BOTTOM_AVG_20-location depend combination {"location"} with aggregator {"SUM", "COUNT"} 
    autoGeneratedKeysToAggregatorIds.put(Sets.newHashSet("location"), Sets.newHashSet(sumId, countId));
    
    //the specific composite aggregator of first combination depends combination {"location", "advertiser", "publisher"} with aggregator {"SUM"}
    autoGeneratedKeysToAggregatorIds.put(Sets.newHashSet("location", "advertiser", "publisher"), Sets.newHashSet(sumId));
    
    //the specific composite aggregator of second combination depends combination {"advertiser", "publisher"} with aggregator {"SUM"}
    autoGeneratedKeysToAggregatorIds.put(Sets.newHashSet("advertiser", "publisher"), Sets.newHashSet(sumId));
    
    //the specific composite aggregator of third combination depends combination {"location", "advertiser", "publisher"} with aggregator {"SUM", "COUNT"}
    autoGeneratedKeysToAggregatorIds.get(Sets.newHashSet("location", "advertiser", "publisher")).addAll(Sets.newHashSet(sumId, countId));
    
    Map<Set<Integer>, Integer> expectedAutoGeneratedIdsToCount = Maps.newHashMap();
    for(Set<Integer> generatorIds : autoGeneratedKeysToAggregatorIds.values())
    {
      Integer count = expectedAutoGeneratedIdsToCount.get(generatorIds);
      if(count == null)
        expectedAutoGeneratedIdsToCount.put(generatorIds, 2);
      else
        expectedAutoGeneratedIdsToCount.put(generatorIds, count+2);
    }
  
    //we can't guarantee the order of automatic generated aggregators
    //List<Set<Integer>> autoGeneratedAggregatorIdsList = Lists.newArrayList(expectedGeneratedCombinationAggregatorIds1, expectedGeneratedCombinationAggregatorIds2, expectedGeneratedCombinationAggregatorIds3);
    Map<Set<Integer>, Integer> autoGeneratedIdsToCount = Maps.newHashMap();
    //ddIDToIncrementalAggregatorIDs is not correct, check it.                
    for(int index=0; index<expectedDdIDNum; ++index)
    {
      IntArrayList incrementalAggregatorIDs = ddIDToIncrementalAggregatorIDs.get(index);
      Set<Integer> incrementalAggregatorIDSet = Sets.newHashSet(incrementalAggregatorIDs.toArray(new Integer[0]));
      Assert.assertTrue("There are duplicate aggregator IDs.", incrementalAggregatorIDs.size() == incrementalAggregatorIDSet.size());
      
      IntArrayList compositeAggregatorIDs = ddIDToCompositeAggregatorIDs.get(index);
      Set<Integer> compositeAggregatorIDSet = Sets.newHashSet(compositeAggregatorIDs.toArray(new Integer[0]));
      Assert.assertTrue("There are duplicate aggregator IDs.", compositeAggregatorIDs.size() == compositeAggregatorIDSet.size());
      
      Set<Integer> allAggregatorIDSet = Sets.newHashSet();
      allAggregatorIDSet.addAll(incrementalAggregatorIDSet);
      allAggregatorIDSet.addAll(compositeAggregatorIDSet);
      Assert.assertTrue("There are overlap aggregator IDs.", allAggregatorIDSet.size() == incrementalAggregatorIDSet.size() + compositeAggregatorIDSet.size());
      
      if(index < 6)
      {
        SetView<Integer> diff1 = Sets.difference(allAggregatorIDSet, expectedAllAggregatorIDSetArray[index]);
        SetView<Integer> diff2 = Sets.difference(expectedAllAggregatorIDSetArray[index], allAggregatorIDSet);
     
        Assert.assertTrue("Not Same aggregator ids. ddID: " + index + "; details: \n" + diff1 + "; " + diff2, 
            diff1.isEmpty() && diff2.isEmpty() );
      }
      else
      {
        Integer count = autoGeneratedIdsToCount.get(allAggregatorIDSet);
        if(count == null)
          autoGeneratedIdsToCount.put(allAggregatorIDSet, 1);
        else
          autoGeneratedIdsToCount.put(allAggregatorIDSet, count+1);
      }
    }
    
    MapDifference<Set<Integer>, Integer> diff = Maps.difference(autoGeneratedIdsToCount, expectedAutoGeneratedIdsToCount);
    Assert.assertTrue(diff.toString(), diff.areEqual());
   
    
    //TODO: verify Input/Output description? maybe future.
  }
  
  /**
   * merge right into left and return left
   * @param left
   * @param right
   * @return
   */
  protected Map<String, FieldsDescriptor> merge(Map<String, FieldsDescriptor> left, Map<String, FieldsDescriptor> right)
  {
    for(Map.Entry<String, FieldsDescriptor> rightEntry : right.entrySet())
    {
      String rightKey = rightEntry.getKey();
      if(left.get(rightKey) == null)
        left.put(rightKey, rightEntry.getValue());
      else
      {
        FieldsDescriptor leftFd = left.get(rightKey);
        Map<String, Type> leftFieldToType = leftFd.getFieldToType();
        Map<String, Type> rightFieldToType = rightEntry.getValue().getFieldToType();
        leftFieldToType.putAll(rightFieldToType);
        leftFd = new FieldsDescriptor(leftFieldToType);
        left.put(rightKey, leftFd);
      }
    }
    return left;
  }
  
  private String produceSchema(String resourceName) throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString(resourceName);

    MessageSerializerFactory dsf = new MessageSerializerFactory(new ResultFormatter());
    DimensionalSchema schemaDimensional = new DimensionalSchema(new DimensionalConfigurationSchema(eventSchemaJSON,
                                                                                           AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    SchemaQuery schemaQuery = new SchemaQuery("1");

    SchemaResult result = new SchemaResult(schemaQuery, schemaDimensional);
    return dsf.serialize(result);
  }

  private List<String> getStringList(JSONArray ja) throws Exception
  {
    List<String> stringsArray = Lists.newArrayList();

    for (int index = 0; index < ja.length(); index++) {
      stringsArray.add(ja.getString(index));
    }

    return stringsArray;
  }

  private void basicSchemaChecker(String resultSchema,
                                  List<String> timeBuckets,
                                  List<String> keyNames,
                                  List<String> keyTypes,
                                  Map<String, String> valueToType,
                                  List<Set<String>> dimensionCombinationsList) throws Exception
  {
    LOG.debug("Schema to check {}", resultSchema);
    JSONObject schemaJO = new JSONObject(resultSchema);
    JSONObject data = schemaJO.getJSONArray("data").getJSONObject(0);

    JSONArray jaBuckets = SchemaUtils.findFirstKeyJSONArray(schemaJO, "buckets");

    Assert.assertEquals(timeBuckets.size(), jaBuckets.length());

    for(int index = 0;
        index < jaBuckets.length();
        index++) {
      Assert.assertEquals(timeBuckets.get(index), jaBuckets.get(index));
    }

    JSONArray keys = data.getJSONArray("keys");

    for(int index = 0;
        index < keys.length();
        index++) {
      JSONObject keyJO = keys.getJSONObject(index);

      Assert.assertEquals(keyNames.get(index), keyJO.get("name"));
      Assert.assertEquals(keyTypes.get(index), keyJO.get("type"));
      Assert.assertTrue(keyJO.has("enumValues"));
    }

    JSONArray valuesArray = data.getJSONArray("values");

    Assert.assertEquals("Incorrect number of values.", valueToType.size(), valuesArray.length());

    Set<String> valueNames = Sets.newHashSet();

    for(int index = 0;
        index < valuesArray.length();
        index++) {
      JSONObject valueJO = valuesArray.getJSONObject(index);

      String valueName = valueJO.getString("name");
      String typeName = valueJO.getString("type");

      String expectedType = valueToType.get(valueName);

      Assert.assertTrue("Duplicate value name " + valueName, valueNames.add(valueName));
      Assert.assertTrue("Invalid value name " + valueName, expectedType != null);

      Assert.assertEquals(expectedType, typeName);
    }

    JSONArray dimensions = data.getJSONArray("dimensions");

    for(int index = 0;
        index < dimensions.length();
        index++) {
      JSONObject combination = dimensions.getJSONObject(index);
      JSONArray dimensionsCombinationArray = combination.getJSONArray("combination");

      Set<String> dimensionCombination = Sets.newHashSet();

      for(int dimensionIndex = 0;
          dimensionIndex < dimensionsCombinationArray.length();
          dimensionIndex++) {
        dimensionCombination.add(dimensionsCombinationArray.getString(dimensionIndex));
      }

      Assert.assertEquals(dimensionCombinationsList.get(index), dimensionCombination);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionalSchemaTest.class);
}
