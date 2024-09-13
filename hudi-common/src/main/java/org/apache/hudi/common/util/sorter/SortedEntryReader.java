/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.sorter;

import java.io.Closeable;
import java.util.Iterator;

public abstract class SortedEntryReader<R> implements Closeable {
  protected final int readerId;
  protected int batchId;
  protected final long batchSize;

  protected SortedEntryReader(int readerId, long batchSize) {
    this.readerId = readerId;
    this.batchSize = batchSize;
  }

  abstract Iterator<R> readBatch();

  abstract void releaseBatch();
}