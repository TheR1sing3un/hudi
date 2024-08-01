package org.apache.hudi.common.util.collection.cache;

public abstract class Cache<K, V> {
  protected final EvictionTriggerStrategy<K, V> evictionTriggerStrategy;

  protected Cache(EvictionTriggerStrategy<K, V> evictionTriggerStrategy) {
    this.evictionTriggerStrategy = evictionTriggerStrategy;
  }

  protected boolean onTriggerEviction() {
    return evictionTriggerStrategy.onTriggered();
  }

  public abstract void put(K key, V value);

  public abstract V get(K key);

  public abstract V remove(K key);

  public abstract void clear();

  public abstract int size();

}

