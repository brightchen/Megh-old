package com.datatorrent.contrib.dimensions;

import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.MutablePair;

import com.google.common.collect.Maps;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.Result;

/**
 * This unifier merge the query result. If any one is not empty, output
 * non-empty. If all are empty, output empty. Note: there probably have multiple
 * quests, and multiple responses
 *
 */
public class DimensionQueryResultMergeUnifier extends BaseOperator implements Unifier<String>
{
  private static final transient Logger logger = LoggerFactory.getLogger(DimensionQueryResultMergeUnifier.class);
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  //id ==> list of data
  /**
   * id ==> MutablePair( non-empty, empty ). currently, only one partition
   * return the non-empty result, all other partitions return emtpy result.
   */
  protected Map<String, MutablePair<String, String>> idToTuplesMap = Maps.newHashMap();

  @Override
  public void beginWindow(long windowId)
  {
    idToTuplesMap.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
    emitMergeResults();
  }

  @Override
  public void process(String tuple)
  {
    JSONObject jo = null;

    try {
      jo = new JSONObject(tuple);
      if (jo.getString(Result.FIELD_TYPE).equals(DataResultDimensional.TYPE)) {
        String id = jo.getString(Result.FIELD_ID);
        JSONArray dataArray = jo.getJSONArray(Result.FIELD_DATA);
        boolean isEmpty = ((dataArray == null) || (dataArray.length() == 0));
        cacheTuple(id, tuple, isEmpty);
      } else {
        logger.info("Invalid type: {}, by pass.", jo.getString(Result.FIELD_TYPE));
        output.emit(tuple);
      }
    } catch (JSONException ex) {
      logger.warn("Invalid json. {}", ex.getMessage());
      throw new RuntimeException(ex);
    }
  }

  /**
   * @param id
   * @param dataArray
   */
  protected void cacheTuple(String id, String tuple, boolean isEmpty)
  {
    logger.info("Cache {}empty tuple.", (isEmpty ? "" : "non-"));
    MutablePair<String, String> pair = idToTuplesMap.get(id);
    if (pair == null) {
      pair = new MutablePair<>();
      idToTuplesMap.put(id, pair);
    }
    if (isEmpty)
      pair.right = tuple;
    else
      pair.left = tuple;
  }

  protected void emitMergeResults()
  {
    for (MutablePair<String, String> tuplePair : idToTuplesMap.values()) {
      logger.info("emit {} tuple.", (tuplePair.left != null ? "non-empty" : "emtpy"));
      output.emit(tuplePair.left != null ? tuplePair.left : tuplePair.right);
    }
  }

}
