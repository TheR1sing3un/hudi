/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.avro.Schema;

import java.util.Objects;
import java.util.Properties;

/**
 * Delete record is a combination of HoodieKey and ordering value.
 * The record is used for {@link org.apache.hudi.common.table.log.block.HoodieDeleteBlock}
 * to support per-record deletions. The deletion block is always appended after the data block,
 * we need to keep the ordering val to combine with the data records when merging, or the data loss
 * may occur if there are intermediate deletions for the inputs
 * (a new INSERT comes after a DELETE in one input batch).
 *
 * NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
 *
 *       This class is serialized (using Kryo) as part of {@code HoodieDeleteBlock} to make
 *       sure this stays backwards-compatible we can't MAKE ANY CHANGES TO THIS CLASS (add,
 *       delete, reorder or change types of the fields in this class, make class final, etc)
 *       as this would break its compatibility with already persisted blocks.
 *
 *       Check out HUDI-5760 for more details
 */
public class DeleteRecord extends BaseHoodieRecord {
  private static final long serialVersionUID = 1L;

  /**
   * For purposes of preCombining.
   */
  private final Comparable<?> orderingVal;

  private DeleteRecord(HoodieKey hoodieKey, Comparable orderingVal) {
    // this.hoodieKey = hoodieKey;
    super(hoodieKey);
    this.orderingVal = orderingVal;
  }

  public static DeleteRecord create(HoodieKey hoodieKey) {
    return create(hoodieKey, 0);
  }

  public static DeleteRecord create(String recordKey, String partitionPath) {
    return create(recordKey, partitionPath, 0);
  }

  public static DeleteRecord create(String recordKey, String partitionPath, Comparable orderingVal) {
    return create(new HoodieKey(recordKey, partitionPath), orderingVal);
  }

  public static DeleteRecord create(HoodieKey hoodieKey, Comparable orderingVal) {
    return new DeleteRecord(hoodieKey, orderingVal);
  }

  public String getRecordKey() {
    return key.getRecordKey();
  }

  public String getPartitionPath() {
    return key.getPartitionPath();
  }

  public HoodieKey getHoodieKey() {
    return key;
  }

  public Comparable<?> getOrderingValue() {
    return orderingVal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeleteRecord)) {
      return false;
    }
    DeleteRecord that = (DeleteRecord) o;
    return this.key.equals(that.key) && this.orderingVal.equals(that.orderingVal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.key, this.orderingVal);
  }

  @Override
  public String toString() {
    return "DeleteRecord {"
            + " key=" + key
            + " orderingVal=" + this.orderingVal
            + '}';
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    return orderingVal;
  }
}
