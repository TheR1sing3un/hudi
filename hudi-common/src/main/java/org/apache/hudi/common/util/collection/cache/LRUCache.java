package org.apache.hudi.common.util.collection.cache;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K, V> extends Cache<K, V> {

  private final Node<K, V> dummyHead, dummyTail;
  private final Map<K, Node<K, V>> cacheMap;

  class Node<K, V> {
    K key;
    V value;
    Node<K, V> prev;
    Node<K, V> next;
    Node(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }

  public LRUCache(EvictionTriggerStrategy<K, V> evictionTriggerStrategy) {
    super(evictionTriggerStrategy);
    this.dummyHead = new Node<>(null, null);
    this.dummyTail = new Node<>(null, null);
    this.dummyHead.next = dummyTail;
    this.dummyTail.prev = dummyHead;
    this.cacheMap = new HashMap<>();
  }

  @Override
  public void put(K key, V value) {
    while (onTriggerEviction()) {
      // evict the last entry
      Node<K, V> evicted = removeTailFromList();
      cacheMap.remove(evicted.key);
      this.evictionTriggerStrategy.onRemove(evicted.key, evicted.value);
    }
    Node<K, V> node = new Node<>(key, value);
    Node<K, V> oldNode = cacheMap.put(key, node);
    addHeadToList(node);
    if (oldNode != null) {
      removeFromList(oldNode);
    }
    this.evictionTriggerStrategy.onPut(key, value, oldNode == null ? null : oldNode.value);
  }

  @Override
  public V get(K key) {
    Node<K, V> node = cacheMap.get(key);
    if (node == null) {
      return null;
    }
    // move the node to the head
    removeFromList(node);
    addHeadToList(node);
    return node.value;
  }

  private void removeFromList(Node node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
  }

  private void addHeadToList(Node node) {
    Node oldHeader = dummyHead.next;
    dummyHead.next = node;
    node.prev = dummyHead;
    node.next = oldHeader;
    oldHeader.prev = node;
  }

  private Node<K, V> removeTailFromList() {
    Node<K, V> eldest = dummyTail.prev;
    removeFromList(eldest);
    return eldest;
  }

  @Override
  public V remove(K key) {
    Node<K, V> node = cacheMap.remove(key);
    if (node == null) {
      return null;
    }
    removeFromList(node);
    this.evictionTriggerStrategy.onRemove(key, node.value);
    return node.value;
  }

  @Override
  public void clear() {
    cacheMap.clear();
    this.evictionTriggerStrategy.onClear();
    this.dummyHead.next = dummyTail;
    this.dummyTail.prev = dummyHead;
  }

  @Override
  public int size() {
    return cacheMap.size();
  }

}
