package com.datatorrent.alerts.action.command;

import java.util.Set;

public interface WhiteListProvider {
  public Set<String> getWhiteList();
  
  /**
   * refresh the provider.
   * @return true is the result changed.
   */
  public boolean refresh();
}
