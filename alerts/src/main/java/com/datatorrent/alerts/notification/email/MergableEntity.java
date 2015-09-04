package com.datatorrent.alerts.notification.email;

public class MergableEntity<T> {
  protected final T entity;
  protected final MergePolicy mergePolicy;

  public MergableEntity(T entity, MergePolicy mergePolicy)
  {
    this.entity = entity;
    this.mergePolicy = mergePolicy;
  }
  
  public MergableEntity(T entity, String mergePolicy)
  {
    this( entity, MergePolicy.fromValue(mergePolicy) );
  }
  
  @Override
  public String toString()
  {
    return String.format("entity: %s; mergePolicy: %s", entity, mergePolicy);
  }
}
