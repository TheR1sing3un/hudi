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

import org.apache.hudi.common.fs.SizeAwareDataOutputStream;
import org.apache.hudi.exception.HoodieIOException;

import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class ExternalSorter<T extends Serializable> implements Closeable, Iterable<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalSorter.class);

  public static final int BUFFER_SIZE = 128 * 1024; // 128 KB

  public static final String DEFAULT_BASE_PATH_PARENT = "/tmp/hudi/external-sorter";

  private final String basePath;

  private final long maxMemoryInBytes;

  private final Comparator<T> comparator;

  private final List<T> memoryRecords;

  // Size Estimator for record
  private final SizeEstimator<T> recordSizeEstimator;
  private int currentSortedFileIndex = 0;
  private long currentMemoryUsage = 0;
  private long totalEntryCount = 0;

  private SizeAwareDataOutputStream writeOnlyFileHandle;
  private File writeOnlyFile;
  private FileOutputStream writeOnlyFileStream;

  private final List<File> currentLevelFiles = new ArrayList<>();

  private Option<File> sortedFile = Option.empty();

  public ExternalSorter(long maxMemoryInBytes, Comparator<T> comparator, SizeEstimator<T> recordSizeEstimator) throws IOException {
    this.maxMemoryInBytes = maxMemoryInBytes;
    this.comparator = comparator;
    this.memoryRecords = Lists.newArrayList();
    this.recordSizeEstimator = recordSizeEstimator;
    this.basePath = DEFAULT_BASE_PATH_PARENT + "/" + System.currentTimeMillis() + "/" + UUID.randomUUID();
    File baseDir = new File(basePath);
    FileIOUtils.deleteDirectory(baseDir);
    FileIOUtils.mkdir(baseDir);
    baseDir.deleteOnExit();
    String parentPathForLevel0 = basePath + "/0";
    String writeFilePath = parentPathForLevel0 + "/" + currentSortedFileIndex;
    this.writeOnlyFile = createFileForWrite(writeFilePath);
    this.writeOnlyFileStream = new FileOutputStream(writeOnlyFile, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(writeOnlyFileStream, BUFFER_SIZE);
  }

  public void add(T record) {
    memoryRecords.add(record);
    totalEntryCount++;
    currentMemoryUsage += recordSizeEstimator.sizeEstimate(record);
    if (currentMemoryUsage > maxMemoryInBytes) {
      LOG.debug("Memory usage {} exceeds maxMemoryInBytes {}. Sorting records in memory.", currentMemoryUsage, maxMemoryInBytes);
      try {
        sortAndWriteToFile();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to sort records", e);
      }
    }
  }

  public long getTotalEntryCount() {
    return totalEntryCount;
  }

  private File createFileForWrite(String filePath) throws IOException {
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdir();
    }
    file.createNewFile();
    LOG.debug("Created file for write: " + file.getAbsolutePath());
    file.deleteOnExit();
    return file;
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int currentFileIndex = 0;
      private int currentRecordIndex = 0;
      private Option<BufferedRandomAccessFile> reader = Option.empty();
      private Option<Entry> currentRecord = Option.empty();
      private boolean init = false;

      @Override
      public boolean hasNext() {
        if (!init) {
          try {
            init();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        if (currentRecord.isPresent()) {
          return true;
        }
        return false;
      }

      private void init() throws IOException {
        if (sortedFile.isPresent()) {
          try {
            reader = Option.of(new BufferedRandomAccessFile(sortedFile.get(), "r"));
            // load first entry
            currentRecord = readEntry(reader.get());
            if (currentRecord.isEmpty()) {
              reader.get().close();
            }
          } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
          }
        } else {
          return;
        }
        init = true;
      }

      @Override
      public T next() {
        if (!init) {
          try {
            init();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        Entry current = currentRecord.get();
        T record = SerializationUtils.deserialize(current.getRecord());
        try {
          currentRecord = readEntry(reader.get());
          if (currentRecord.isEmpty()) {
            reader.get().close();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return record;
      }
    };
  }

  private void sortAndWriteToFile() throws IOException {
    memoryRecords.sort(comparator);
    for (T record : memoryRecords) {
      Entry entry = Entry.newEntry(SerializationUtils.serialize(record));
      entry.writeToFile(writeOnlyFileHandle);
    }
    writeOnlyFileHandle.flush();
    writeOnlyFileHandle.close();
    currentLevelFiles.add(writeOnlyFile);
    memoryRecords.clear();
    currentMemoryUsage = 0;
    currentSortedFileIndex++;
    String writeFilePath = this.basePath + "/0/" + currentSortedFileIndex;
    writeOnlyFile = createFileForWrite(writeFilePath);
    writeOnlyFileStream = new FileOutputStream(writeOnlyFile, true);
    writeOnlyFileHandle = new SizeAwareDataOutputStream(writeOnlyFileStream, BUFFER_SIZE);
  }

  public void sort() {
    try {
      // sort the remaining records in memory
      // TODO: don't need to flush to disk when there never exceed memory limit
      if (!memoryRecords.isEmpty()) {
        sortAndWriteToFile();
      }
      // merge the sorted files
      sortedMerge();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to sort records", e);
    }
  }

  private void sortedMerge() throws IOException {
    int level = 0;
    int index = 0;
    while (currentLevelFiles.size() > 1) {
      List<File> nextLevelFiles = new ArrayList<>();
      List<File> mergedFiles = new ArrayList<>();
      for (int i = 0; i < currentLevelFiles.size(); i += 2) {
        if (i + 1 < currentLevelFiles.size()) {
          File file1 = currentLevelFiles.get(i);
          File file2 = currentLevelFiles.get(i + 1);
          nextLevelFiles.add(mergeTwoFiles(file1, file2, level, index++));
          mergedFiles.add(file1);
          mergedFiles.add(file2);
        } else {
          // TODO: add the last file to first
          nextLevelFiles.add(currentLevelFiles.get(i));
        }
      }
      // TODO: reduce data movement
      mergedFiles.forEach(File::delete);
      currentLevelFiles.clear();
      currentLevelFiles.addAll(nextLevelFiles);
      level++;
      index = 0;
    }
    sortedFile = Option.of(currentLevelFiles.get(0));
  }

  private File mergeTwoFiles(File file1, File file2, int level, int index) throws IOException {
    BufferedRandomAccessFile reader1 = new BufferedRandomAccessFile(file1, "r");
    BufferedRandomAccessFile reader2 = new BufferedRandomAccessFile(file2, "r");
    String filePath = this.basePath + "/" + (level + 1) + "-" + index;
    File mergedFile = createFileForWrite(filePath);
    FileOutputStream fileOutputStream = new FileOutputStream(mergedFile, true);
    SizeAwareDataOutputStream outputStream = new SizeAwareDataOutputStream(fileOutputStream, BUFFER_SIZE);
    Option<Entry> entry1 = readEntry(reader1);
    Option<Entry> entry2 = readEntry(reader2);
    while (entry1.isPresent() && entry2.isPresent()) {
      // pick the smaller one
      int compareResult = comparator.compare(SerializationUtils.deserialize(entry1.get().getRecord()), SerializationUtils.deserialize(entry2.get().getRecord()));
      if (compareResult < 0) {
        entry1.get().writeToFile(outputStream);
        entry1 = readEntry(reader1);
      } else {
        entry2.get().writeToFile(outputStream);
        entry2 = readEntry(reader2);
      }
    }
    while (entry1.isPresent()) {
      entry1.get().writeToFile(outputStream);
      entry1 = readEntry(reader1);
    }
    while (entry2.isPresent()) {
      entry2.get().writeToFile(outputStream);
      entry2 = readEntry(reader2);
    }
    outputStream.flush();
    outputStream.close();
    reader1.close();
    reader2.close();
    return mergedFile;
  }

  private Option<Entry> readEntry(BufferedRandomAccessFile reader) throws IOException {
    int magic;
    try {
      magic = reader.readInt();
    } catch (EOFException e) {
      // reach end
      return Option.empty();
    }
    if (magic != Entry.MAGIC) {
      throw new IOException("Invalid magic number " + magic);
    }
    long crc = reader.readLong();
    int recordSize = reader.readInt();
    byte[] record = new byte[recordSize];
    reader.readFully(record, 0, recordSize);
    // check crc
    long crcOfReadValue = BinaryUtil.generateChecksum(record);
    if (crc != crcOfReadValue) {
      throw new IOException("Checksum mismatch");
    }
    return Option.of(new Entry(magic, crc, recordSize, record));
  }

  @Override
  public void close() {
    try {
      if (writeOnlyFileHandle != null) {
        writeOnlyFileHandle.close();
      }
      if (writeOnlyFileStream != null) {
        writeOnlyFileStream.close();
      }
      if (writeOnlyFile != null) {
        writeOnlyFile.delete();
      }
      currentLevelFiles.forEach(File::delete);
      sortedFile.ifPresent(File::delete);
      memoryRecords.clear();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close external sorter", e);
    }
  }


  public static final class Entry {
    public static final int MAGIC = 0x123321;
    private Integer magic;
    private Long crc;
    private Integer recordSize;
    private byte[] record;

    public Entry(Integer magic, Long crc, Integer recordSize, byte[] record) {
      this.magic = magic;
      this.crc = crc;
      this.recordSize = recordSize;
      this.record = record;
    }

    public Integer getMagic() {
      return magic;
    }

    public Long getCrc() {
      return crc;
    }

    public Integer getRecordSize() {
      return recordSize;
    }

    public byte[] getRecord() {
      return record;
    }

    public static Entry newEntry(byte[] record) {
      return new Entry(MAGIC, BinaryUtil.generateChecksum(record), record.length, record);
    }

    public void writeToFile(SizeAwareDataOutputStream outputStream) throws IOException {
      outputStream.writeInt(magic);
      outputStream.writeLong(crc);
      outputStream.writeInt(recordSize);
      outputStream.write(record);
    }
  }


}
