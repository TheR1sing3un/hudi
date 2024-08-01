package org.apache.hudi.common.util.collection.cache;

public class SumEvictionTriggerStrategy<K, V> implements EvictionTriggerStrategy<K, V> {

  private final long maxSum;
  private long currentSum;

  public SumEvictionTriggerStrategy(long maxSum) {
    this.maxSum = maxSum;
    this.currentSum = 0;
  }


  @Override
  public void onPut(K key, V newValue, V oldValue) {
    currentSum += oldValue == null ? 1 : 0;
  }

  @Override
  public void onRemove(K key, V value) {
    if (value == null) {
      return;
    }
    currentSum--;
  }

  @Override
  public boolean onTriggered() {
    return currentSum > maxSum;
  }

  @Override
  public void onClear() {
    currentSum = 0;
  }
}
