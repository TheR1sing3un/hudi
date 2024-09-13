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

package org.apache.hudi.table.action.compact.sort.merge;

import org.apache.hudi.common.model.CompactionContext;
import org.apache.hudi.config.HoodieWriteConfig;

public class OptimizeSortMergeCompactionTriggerStrategy implements SortMergeCompactionTriggerStrategy {
  @Override
  public boolean trigger(CompactionContext context, HoodieWriteConfig config) {
    if (!context.hasBaseFile()) {
      // no base-file, we can trigger sort merge join compaction for the first time to generate the sorted base file
      return true;
    }
    if (context.isBaseFileSorted()) {
      // base-file is already sorted, we don't need to sort the base-file again, so pick sort merge join compaction for optimization
      return true;
    }

    // base-file exists but not sorted, we need to evaluate performance
    // TODO: complete this logic when we have more performance data
    return true;
  }
}