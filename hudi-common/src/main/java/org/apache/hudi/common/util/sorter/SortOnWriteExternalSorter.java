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

import org.apache.hudi.common.fs.SizeAwareDataOutputStream;
import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

// TODO: Just a simple external-sorter, optimize it later
public class SortOnWriteExternalSorter<R extends Serializable> extends ExternalSorter<R> {

  private static final Logger LOG = LoggerFactory.getLogger(SortOnWriteExternalSorter.class);

  // TODO: configure an appropriate buffer size
  public static final int BUFFER_SIZE = 128 * 1024; // 128 KB

  private int readBuffer = BUFFER_SIZE;

  // A timer for calculating elapsed time in millis
  private final HoodieTimer timer = HoodieTimer.create();

  private final List<R> memoryRecords;
  private int currentSortedFileIndex = 0;
  private long currentMemoryUsage = 0;
  private long totalMemoryUsage = 0;
  private long totalFileSize = 0;
  private long totalTimeTakenToSortRecords;

  private SizeAwareDataOutputStream writeOnlyFileHandle;
  private File writeOnlyFile;
  private FileOutputStream writeOnlyFileStream;

  private final List<File> currentLevelFiles = new ArrayList<>();

  private Option<File> sortedFile = Option.empty();

  public SortOnWriteExternalSorter(String baseFilePath, long maxMemoryInBytes, Comparator<R> comparator, SizeEstimator<R> recordSizeEstimator) throws IOException {
    super(baseFilePath, maxMemoryInBytes, comparator, recordSizeEstimator);
    this.memoryRecords = new LinkedList<>();
  }

  public long getTotalTimeTakenToSortRecords() {
    return totalTimeTakenToSortRecords;
  }

  public int getGeneratedSortedFileNum() {
    return currentSortedFileIndex + 1;
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

  private void sortAndWriteToFile() throws IOException {
    // TODO: consider merge during sort
    // 1. sort in memory
    memoryRecords.sort(comparator);

    // 2. create current write handle
    createNewWriteFile(0, currentSortedFileIndex++);

    // 3. write every record to file
    for (R record : memoryRecords) {
      Entry entry = Entry.newEntry(SerializationUtils.serialize(record));
      entry.writeToFile(writeOnlyFileHandle);
    }

    // 4. add current write file to current level files
    currentLevelFiles.add(writeOnlyFile);

    // 5. flush and close current write handle
    closePreviousWriteFile();

    // 6. clear memory records
    memoryRecords.clear();
    currentMemoryUsage = 0;
  }

  private void createNewWriteFile(int level, int index) throws IOException {
    String writeFilePath = fileNameGenerate(level, index);
    writeOnlyFile = createFileForWrite(writeFilePath);
    writeOnlyFileStream = new FileOutputStream(writeOnlyFile, true);
    writeOnlyFileHandle = new SizeAwareDataOutputStream(writeOnlyFileStream, BUFFER_SIZE);
  }

  private void closePreviousWriteFile() throws IOException {
    if (writeOnlyFileHandle != null) {
      totalFileSize += writeOnlyFileHandle.getSize();
      writeOnlyFileHandle.flush();
      writeOnlyFileHandle.close();
    }
  }

  private String fileNameGenerate(int level, int index) {
    return String.format("%s/%d-%d", this.basePath, level, index);
  }

  private void sort() {
    try {
      // sort the remaining records in memory
      // TODO: don't need to flush to disk when there never exceed memory limit
      if (!memoryRecords.isEmpty()) {
        if (currentSortedFileIndex == 0) {
          // there are never exceed memory limit, only sort the memory records
          sortMemoryRecords();
          LOG.info("SortOnWriteExternalSorter sort completed, all in memory,"
                  + "total entry count => {}, total time taken to sort records => {} ms,"
                  + "generated sorted file num => {}, total memory usage => {} bytes, total file write size => {} bytes",
              totalEntryCount, totalTimeTakenToSortRecords, getGeneratedSortedFileNum(), totalMemoryUsage, totalFileSize);
          return;
        }
        // there has happened sort and write to file
        sortAndWriteToFile();
      }
      // merge the sorted files
      sortedMerge();
      LOG.info("SortOnWriteExternalSorter sort completed,"
              + "total entry count => {}, total time taken to sort records => {} ms,"
              + "generated sorted file num => {}, total memory usage => {} bytes, total file write size => {} bytes",
          totalEntryCount, totalTimeTakenToSortRecords, getGeneratedSortedFileNum(), totalMemoryUsage, totalFileSize);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to sort records", e);
    }
  }

  private void sortMemoryRecords() {
    this.timer.startTimer();
    memoryRecords.sort(comparator);
    this.totalTimeTakenToSortRecords = this.timer.endTimer();
  }

  private void sortedMerge() throws IOException {
    this.timer.startTimer();
    int level = 0;
    int index = 0;
    while (currentLevelFiles.size() > 1) {
      List<File> nextLevelFiles = new ArrayList<>();
      for (int i = 0; i < currentLevelFiles.size(); i += 2) {
        if (i + 1 < currentLevelFiles.size()) {
          File file1 = currentLevelFiles.get(i);
          File file2 = currentLevelFiles.get(i + 1);
          nextLevelFiles.add(mergeTwoFiles(file1, file2, level, index++));
        } else {
          // TODO: consider add the last file to first
          File file = currentLevelFiles.get(i);
          if (index > 0) {
            File nextLevelFile = new File(fileNameGenerate(level + 1, index));
            file.renameTo(nextLevelFile);
            LOG.debug("Rename file {} to {}", file.getAbsolutePath(), nextLevelFile.getAbsolutePath());
            file = nextLevelFile;
          }
          nextLevelFiles.add(file);
        }
      }
      // TODO: reduce data movement
      currentLevelFiles.clear();
      currentLevelFiles.addAll(nextLevelFiles);
      level++;
      index = 0;
    }
    sortedFile = Option.of(currentLevelFiles.get(0));
    this.totalTimeTakenToSortRecords = this.timer.endTimer();
  }

  private File mergeTwoFiles(File file1, File file2, int level, int index) throws IOException {
    BufferedRandomAccessFile reader1 = new BufferedRandomAccessFile(file1, "r", readBuffer);
    BufferedRandomAccessFile reader2 = new BufferedRandomAccessFile(file2, "r", readBuffer);
    String filePath = fileNameGenerate(level + 1, index);
    File mergedFile = createFileForWrite(filePath);
    FileOutputStream fileOutputStream = new FileOutputStream(mergedFile, true);
    SizeAwareDataOutputStream outputStream = new SizeAwareDataOutputStream(fileOutputStream, BUFFER_SIZE);
    Option<Entry> entry1 = readEntry(reader1);
    Option<Entry> entry2 = readEntry(reader2);
    while (entry1.isPresent() && entry2.isPresent()) {
      // pick the smaller one
      int compareResult = comparator.compare(SerializationUtils.deserialize(entry1.get().getRecord()), SerializationUtils.deserialize(entry2.get().getRecord()));
      // left <= right, pick left, because left's natural order is smaller
      if (compareResult <= 0) {
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
    file1.delete();
    file2.delete();
    return mergedFile;
  }

  private Option<Entry> readEntry(BufferedRandomAccessFile reader) {
    int magic;
    try {
      magic = reader.readInt();
    } catch (IOException e) {
      // reach end
      return Option.empty();
    }
    if (magic != Entry.MAGIC) {
      throw new HoodieIOException("Invalid magic number");
    }
    try {
      long crc = reader.readLong();
      int recordSize = reader.readInt();
      byte[] record = new byte[recordSize];
      reader.readFully(record, 0, recordSize);
      // check crc
      long crcOfReadValue = BinaryUtil.generateChecksum(record);
      if (crc != crcOfReadValue) {
        throw new HoodieIOException("CRC mismatch");
      }
      return Option.of(new Entry(magic, crc, recordSize, record));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read entry", e);
    }
  }

  @Override
  public void closeSorter() {
    try {
      if (writeOnlyFileHandle != null) {
        writeOnlyFileHandle.close();
      }
      if (writeOnlyFile != null) {
        writeOnlyFile.delete();
      }
      currentLevelFiles.forEach(File::delete);
      sortedFile.ifPresent(File::delete);
      memoryRecords.clear();
      currentMemoryUsage = 0;

      LOG.info("SortOnWriteExternalSorter closed,"
              + "total entry count => {}, total time taken to sort records => {} ms,"
              + "generated sorted file num => {}, total memory usage => {} bytes, total file write size => {} bytes",
          totalEntryCount, totalTimeTakenToSortRecords, getGeneratedSortedFileNum(), totalMemoryUsage, totalFileSize);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close external sorter", e);
    }
  }

  @Override
  protected void addInner(R record) {
    memoryRecords.add(record);
    long sizeEstimated = sizeEstimate(record);
    currentMemoryUsage += sizeEstimated;
    totalMemoryUsage += sizeEstimated;
    if (currentMemoryUsage > maxMemoryInBytes) {
      LOG.debug("Memory usage {} exceeds maxMemoryInBytes {}. Sorting records in memory.", currentMemoryUsage, maxMemoryInBytes);
      try {
        sortAndWriteToFile();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to sort records", e);
      }
    }
  }

  @Override
  protected void finishInner() {
    // trigger sort
    sort();
  }

  @Override
  protected ClosableIterator<R> getIteratorInner() {
    return sortedFile.isPresent() ? new SortedRecordInDiskIterator<>(sortedFile.get()) : ClosableIterator.wrap(memoryRecords.iterator());
  }

  private class SortedRecordInDiskIterator<R> implements ClosableIterator<R> {

    private final BufferedRandomAccessFile reader;
    private Option<Entry> currentRecord = Option.empty();
    private int iterateCount = 0;

    public SortedRecordInDiskIterator(File sortedFile) {
      try {
        reader = new BufferedRandomAccessFile(sortedFile, "r", BUFFER_SIZE);
        // load first entry
        currentRecord = readEntry(reader);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read sorted file", e);
      }
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close reader", e);
      }
      LOG.debug("Total iterate count: " + iterateCount);
    }

    @Override
    public boolean hasNext() {
      if (currentRecord.isPresent()) {
        return true;
      }
      return false;
    }

    @Override
    public R next() {
      iterateCount++;
      Entry current = currentRecord.get();
      R record = SerializationUtils.deserialize(current.getRecord());
      currentRecord = readEntry(reader);
      return record;
    }
  }

  private static final class Entry {
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