package org.apache.hudi.common.model;

import org.apache.avro.Schema;

import java.util.Properties;

public abstract class BaseHoodieRecord {
  protected HoodieKey key;
  protected BaseHoodieRecord(HoodieKey key) {
    this.key = key;
  }

  public HoodieKey getKey() {
    return key;
  }

  public void setKey(HoodieKey key) {
    this.key = key;
  }

  public abstract Comparable<?> getOrderingValue(Schema recordSchema, Properties props);
}
