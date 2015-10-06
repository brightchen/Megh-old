package com.datatorrent.demos.dimensions.telecom.generate;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.bval.jsr303.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class keep the map between location(lan, lon) and zipcode.
 * for simplicity, we treat the area of zipcode is a circle. 
 * and this class keep the middle point of the circle of the area.
 * If a point most close to the middle point of the circle, we treat it as in this area
 * 
 * @author bright
 *
 */
public class PointZipCodeRepo {
  private static final transient Logger logger = LoggerFactory.getLogger(PointZipCodeRepo.class);
  
  private final String LOCATION_ZIPS_FILE = "usLocationToZips.csv";
  
  public static class Point
  {
    public static final int SCALAR = 10000;
    //instead of use float, use int to increase performance
    public final int lan;
    public final int lon;
    
    private Point()
    {
      lan = 0;
      lon = 0;
    }
    
    public Point(int lan, int lon)
    {
      this.lan = lan;
      this.lon = lon;
    }
    
    public Point(float lan, float lon)
    {
      this.lan = (int)(lan*SCALAR);
      this.lon = (int)(lon*SCALAR);
    }

  }
  
  private static PointZipCodeRepo instance = null;
  
  private PointZipCodeRepo(){}
  
  public static PointZipCodeRepo instance()
  {
    if(instance == null)
    {
      synchronized(PointZipCodeRepo.class)
      {
        if(instance == null)
        {
          instance = new PointZipCodeRepo();
          instance.load();
        }
      }
    }
    return instance;
  }
  
  protected SortedMap<Point, Integer> pointToZip = Maps.newTreeMap(new Comparator<Point>(){

    @Override
    public int compare(Point l1, Point l2) {
      if(l1.lan < l2.lan)
        return -1;
      else if(l1.lan > l2.lan)
        return 1;
      else if(l1.lon < l2.lon)
        return -1;
      else if(l1.lon > l2.lon)
        return 1;
      return 0;
    }
    
  });
  
  protected SortedMap<Integer, int[]> lanToLons = Maps.newTreeMap();
  protected int[] sortedLans;
  
  /**
   * load from csv file
   * @throws IOException 
   */
  protected void load()
  {
    URL fileUrl = Thread.currentThread().getContextClassLoader().getResource(LOCATION_ZIPS_FILE);
    if(fileUrl == null)
      return;
    
    String filePath = fileUrl.getPath();

    BufferedReader br = null;
    try {
      FileInputStream fis = new FileInputStream(filePath);
      br = new BufferedReader(new InputStreamReader(fis));
      while(true)
      {
        String line = br.readLine();
        if(line == null)
          break;

        addPointMeta(line);
      }
      
      //generate LanToLons
      Map<Integer, List<Integer>> lanToLonList = Maps.newHashMap();
      Set<Point> points = pointToZip.keySet();
      for(Point point : points )
      {
        List<Integer> lons = lanToLonList.get(point.lan);
        if(lons == null)
        {
          lons = Lists.newArrayList(point.lon);
          lanToLonList.put(point.lan, lons);
        }
        else
          lons.add(point.lon);
      }
      
      
      List<Integer> lans = new ArrayList(lanToLonList.keySet());
      Collections.sort(lans);
      
      sortedLans = toArray(lans);
      
      //sort all the lons and convert Lon list to array
      for(Map.Entry<Integer, List<Integer>> entry : lanToLonList.entrySet())
      {
        Collections.sort( entry.getValue() );
        lanToLons.put(entry.getKey(), toArray(entry.getValue() ));
      }
      
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    finally
    {
      if(br != null)
        IOUtils.closeQuietly(br);
    }
  }
  
  protected static int[] toArray(List<Integer> list)
  {
    int[] array = new int[list.size()];
    int index = 0;
    for(int value : list)
    {
      array[index++] = value;
    }
    return array;
  }
  /**
   * csv format
   * @param line
   */
  public void addPointMeta(String line)
  {
    line = line.trim();
    if(line.isEmpty() || line.startsWith("#"))
      return;
    String[] items = line.split(",");
    try
    {
      addLocaationMeta(Float.valueOf(trimItem(items[2])), Float.valueOf(trimItem(items[3])), Integer.valueOf(trimItem(items[0])));
    }
    catch(Exception e)
    {
      logger.warn("Invalid line: {}", line);
    }
  }
  
  /**
   * get rid possible space, quote etc
   * @param item
   * @return
   */
  public static String trimItem(String item)
  {
    if(item == null || item.isEmpty())
      return item;
    item = item.trim();
    if(item.startsWith("\'") || item.startsWith("\""))
      item = item.substring(1);
    if(item.endsWith("\'") || item.endsWith("\""))
      item = item.substring(0, item.length()-1);
    return item.trim();
  }
  
  public void addLocaationMeta(float lan, float lon, int zip)
  {
    Point location = new Point(lan, lon);
    pointToZip.put(location, zip);
  }
  
  /**
   * get zip by lan and lon
   * @param lan
   * @param lon
   * @return
   */
  public Integer getZip(float lan, float lon)
  {
    int iLan = (int)(lan * Point.SCALAR);
    int iLon = (int)(lon * Point.SCALAR);
  
    return getZipByScaledPoint(iLan, iLon);
  }
  
  private static final int[] stepSizes = new int[]{1, -1};
  public Integer getZipByScaledPoint(int lan, int lon)
  {
    final int lanIndex = getIndexOfMostCloseToLan(lan);
    Long minDistanceSquare = Long.MAX_VALUE;
    int candidateLan = 0;
    int candidateLon = 0;
    for( int stepSize : stepSizes)
    {
      if(minDistanceSquare == 0)
        break;
      for(int index=lanIndex; minDistanceSquare > 0; index+=stepSize)
      {
        final int closeLon = findMostCloseLon(sortedLans[index], lon);
        long distanceSquare = distanceSquare(lan, lon, sortedLans[index], closeLon);
        if(distanceSquare < minDistanceSquare)
        {
          minDistanceSquare = distanceSquare;
          candidateLan = sortedLans[index];
          candidateLon = closeLon;
        }
        if(distanceSquare(lan, sortedLans[index]) >= minDistanceSquare)
          break;
      }
    }
    return pointToZip.get(getPoint(candidateLan, candidateLon));
  }
  
  //distance of one dimension
  public static long distanceSquare(int value, int value1)
  {
    long diffValue = value1 - value;
    return diffValue*diffValue;
  }
  
  public static long distanceSquare(int lan, int lon, int lan1, int lon1)
  {
    long diffLan = lan1 - lan;
    long diffLon = lon1 - lon;
    return diffLan*diffLan + diffLon*diffLon;
  }
  
  public Integer getZip(Point point)
  {
    return getZipByScaledPoint(point.lan, point.lon);
  }
  
  protected Point getPoint(int lan, int lon)
  {
    return new Point(lan, lon);
  }
  
  public int getIndexOfMostCloseToLan(int matchLan)
  {
    return getIndexOfMostCloseToValue(sortedLans, matchLan);
  }
  
  public int findMostCloseLon(int lan, int matchLon)
  {
    return lanToLons.get(lan)[getIndexOfMostCloseToValue(lanToLons.get(lan), matchLon)];
  }
  
  public int getIndexOfMostCloseToLon(int lan, int matchLon)
  {
    return getIndexOfMostCloseToValue(lanToLons.get(lan), matchLon);
  }
  
  public static int getIndexOfMostCloseToValue(int[] values, int matchValue)
  {
    return getIndexOfMostCloseToValue(values, matchValue, 0, values.length-1);
  }
  
  public static int getIndexOfMostCloseToValue(int[] values, int matchValue, int start, int end)
  {
    if(values == null || values.length == 0)
      throw new IllegalArgumentException("Input values should not null or empty.");
    
    if(start == end)
      return start;
    if(start+1 == end)
    {
      return Math.abs(values[start] - matchValue) < Math.abs(values[end] - matchValue) ? start : end;
    }
    
    if(matchValue <= values[start])
    {
      return start;
    }
    if(matchValue >= values[end])
    {
      return end;
    }
    
    
    int middleIndex = (start + end)/2;
    if(matchValue == values[middleIndex] )
      return middleIndex;
    if(matchValue > values[middleIndex] )
      return getIndexOfMostCloseToValue(values, matchValue, middleIndex, end);
    else
      return getIndexOfMostCloseToValue(values, matchValue, start, middleIndex);
  }
}
