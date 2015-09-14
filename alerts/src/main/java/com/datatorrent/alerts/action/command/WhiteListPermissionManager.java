package com.datatorrent.alerts.action.command;

import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Sets;

public class WhiteListPermissionManager implements PermissionManager {
  
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Set<String> whiteList;
  
  private WhiteListProvider whiteListProvider = createWhiteListProvider();
  
  public WhiteListPermissionManager()
  {
    setWhiteList(whiteListProvider.getWhiteList());
  }
  
  @Override
  public void verifyPermission(String appName, String command) throws NoPermissionException {
    boolean contains = false;
    rwLock.readLock().lock();
    try
    {
      contains = whiteList.contains(command);
    }
    catch(Exception e)
    {
    }
    finally
    {
      rwLock.readLock().unlock();
    }
    
    if( contains )
      return;
    throw new NoPermissionException("Command '" + command + "' is not in the white list");
  }

  public void setWhiteList(Set<String> whiteList)
  {
    //the list should not expose out side, clone the input one
    Set<String> wl = Sets.newHashSet();
    for(String entry : whiteList )
    {
      wl.add(entry);
    }
    
    rwLock.writeLock().lock();
    this.whiteList = wl;
    rwLock.writeLock().unlock();
  }
  
  protected WhiteListProvider createWhiteListProvider()
  {
    return new DefaultWhiteListProvider();
  }
  
  public void refresh()
  {
    if(whiteListProvider.refresh())
      setWhiteList(whiteListProvider.getWhiteList());
  }
}
