package com.datatorrent.alerts.action.command;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WhiteListPermissionManager implements PermissionManager {
  
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Set<String> whiteList = new HashSet<String>();
  
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

  protected void setWhiteList(Set<String> whiteList)
  {
    rwLock.writeLock().lock();
    this.whiteList = whiteList;
    rwLock.writeLock().unlock();
  }
}
