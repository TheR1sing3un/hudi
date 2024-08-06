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

package org.apache.hudi.common.util;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TestExternalSorter {

  private static class Record implements Comparable<Record>, Serializable {
    private final int key;
    private final String value;

    public Record(int key, String value) {
      this.key = key;
      this.value = value;
    }

    public int getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    @Override
    public int compareTo(@NotNull TestExternalSorter.Record o) {
      return Integer.compare(key, o.key);
    }
  }

  @TempDir
  private File tmpDir;

  @Test
  public void testSort() throws IOException {
    // generate some random records
    SizeEstimator<Record> sizeEstimator = record -> 4 + 4 + 4 + record.getValue().length();
    int totalNum = 1 << 10;
    Random random = new Random();
    List<Record> records = random.ints(totalNum, 0, totalNum / 20).mapToObj(key -> new Record(key, "value")).collect(Collectors.toList());
    int totalSize = totalNum * (4 + 4 + 4 + 5);
    List<Record> sortedList = records.stream().sorted().collect(Collectors.toList());
    ExternalSorter<Record> sorter = new ExternalSorter<Record>(tmpDir.getPath(), totalSize / 100, Record::compareTo, sizeEstimator);
    for (Record record : records) {
      sorter.add(record);
    }
    sorter.sort();
    Iterator<Record> iterator = sorter.iterator();
    for (Record record : sortedList) {
      Record next = iterator.next();
      assert record.getKey() == next.getKey() && record.getValue().equals(next.getValue());
    }
  }

}
