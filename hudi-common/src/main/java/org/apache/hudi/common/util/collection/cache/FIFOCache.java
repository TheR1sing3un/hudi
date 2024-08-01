package org.apache.hudi.common.util.collection.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class FIFOCache<K, V> extends Cache<K, V> {

  private final Queue<K> accessOrder;

  private final Map<K, V> cacheMap;

  public FIFOCache(EvictionTriggerStrategy<K, V> evictionTriggerStrategy) {
    super(evictionTriggerStrategy);
    this.accessOrder = new LinkedList<>();
    this.cacheMap = new HashMap<>();
  }

  @Override
  public void put(K key, V value) {
    while (onTriggerEviction()) {
      // evict the eldest entry
      K eldestKey = accessOrder.poll();
      V removed = cacheMap.remove(eldestKey);
      this.evictionTriggerStrategy.onRemove(eldestKey, removed);
    }
    V oldValue = cacheMap.put(key, value);
    if (oldValue != null) {
      // remove the key from the access order
      accessOrder.remove(key);
    }
    accessOrder.add(key);
    this.evictionTriggerStrategy.onPut(key, value, oldValue);
  }

  @Override
  public V get(K key) {
    return cacheMap.get(key);
  }

  @Override
  public V remove(K key) {
    V value = cacheMap.remove(key);
    if (value != null) {
      accessOrder.remove(key);
    }
    this.evictionTriggerStrategy.onRemove(key, value);
    return value;
  }

  @Override
  public void clear() {
    cacheMap.clear();
    accessOrder.clear();
    this.evictionTriggerStrategy.onClear();
  }

  @Override
  public int size() {
    return cacheMap.size();
  }
}
