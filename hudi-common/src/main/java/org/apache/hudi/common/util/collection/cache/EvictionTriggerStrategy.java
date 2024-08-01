package org.apache.hudi.common.util.collection.cache;

public interface EvictionTriggerStrategy<K, V> {
  void onPut(K key, V newValue, V oldValue);
  void onRemove(K key, V value);
  boolean onTriggered();
  void onClear();
}
