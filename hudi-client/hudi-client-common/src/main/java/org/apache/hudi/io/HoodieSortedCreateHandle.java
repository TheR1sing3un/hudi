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

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieFileFooterSupport;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

public class HoodieSortedCreateHandle<T, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSortedCreateHandle.class);

  private Iterator<HoodieRecord> unmergedRecordsIter;

  public HoodieSortedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                  String partitionPath, String fileId, Iterator<HoodieRecord> recordItr,
                                  TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, Collections.EMPTY_MAP, taskContextSupplier);
    unmergedRecordsIter = recordItr;
  }

  @Override
  public void write() {
    // Iterate all unmerged records and write them out
    if (!unmergedRecordsIter.hasNext()) {
      LOG.info("No unmerged records to write for partition path " + partitionPath + " and fileId " + fileId);
      return;
    }
    while (unmergedRecordsIter.hasNext()) {
      writeRecord(unmergedRecordsIter.next());
    }
  }

  @Override
  protected void closeInner() {
    // add metadata about sorted
    fileWriter.writeFooterMetadata(HoodieFileFooterSupport.HOODIE_BASE_FILE_SORTED, "true");
    if (unmergedRecordsIter instanceof ClosableIterator) {
      ((ClosableIterator) unmergedRecordsIter).close();
    }
    LOG.info("SortedCreateHandle for partitionPath " + partitionPath + " fileID " + fileId + " wrote with sorted records");
  }
}
