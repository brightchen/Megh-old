package com.datatorrent.alerts.notification.email;

public class MergableEntity<T> {
  protected T entity;
  protected MergePolicy mergePolicy;

  public MergableEntity(T entity, MergePolicy mergePolicy)
  {
    this.entity = entity;
    this.mergePolicy = mergePolicy;
  }
  
  public MergableEntity(T entity, String mergePolicy)
  {
    this( entity, MergePolicy.fromValue(mergePolicy) );
  }
}
