package org.apache.hudi.common.util.collection.cache;

import org.apache.hudi.common.util.SizeEstimator;

public class SizeEvictionTriggerStrategy<K, V> implements EvictionTriggerStrategy<K, V> {

  private final long maxMemorySize;
  // Size Estimator for key type
  private final SizeEstimator<K> keySizeEstimator;
  // Size Estimator for key types
  private final SizeEstimator<V> valueSizeEstimator;
  private long currentMemorySize;

  public SizeEvictionTriggerStrategy(long maxMemorySize, SizeEstimator<K> keySizeEstimator, SizeEstimator<V> valueSizeEstimator) {
    this.maxMemorySize = maxMemorySize;
    this.keySizeEstimator = keySizeEstimator;
    this.valueSizeEstimator = valueSizeEstimator;
    this.currentMemorySize = 0;
  }


  @Override
  public void onPut(K key, V newValue, V oldValue) {
    currentMemorySize += oldValue == null ?
        keySizeEstimator.sizeEstimate(key) + valueSizeEstimator.sizeEstimate(newValue) :
        valueSizeEstimator.sizeEstimate(newValue) - valueSizeEstimator.sizeEstimate(oldValue);
  }

  @Override
  public void onRemove(K key, V value) {
    if (value == null) {
      return;
    }
    currentMemorySize -= keySizeEstimator.sizeEstimate(key) + valueSizeEstimator.sizeEstimate(value);
  }

  @Override
  public boolean onTriggered() {
    return currentMemorySize > maxMemorySize;
  }

  @Override
  public void onClear() {
    currentMemorySize = 0;
  }
}
